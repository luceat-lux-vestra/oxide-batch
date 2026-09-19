//! Attempt-local live scoped-component construction and cleanup.
//!
//! #276 owns durable selector resolution/provenance. This module consumes only
//! already-resolved inputs and explicit application factory registrations. It
//! deliberately owns no ambient lookup, repository authority, or executor.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use futures_util::FutureExt;

use crate::{
    BoxFuture, ComponentRevision, MAX_SCOPED_COMPONENTS, ParameterName, ParameterValue,
    ScopeFactoryKind, ScopeKind, ScopedComponentId,
};

/// Maximum dependency-chain depth accepted for one live component scope.
pub const MAX_SCOPED_DEPENDENCY_DEPTH: usize = 32;

/// Opaque process-local handle to one live scoped component.
///
/// The value is never serialized; applications may recover the concrete type
/// with [`Self::downcast_ref`].
#[derive(Clone)]
pub struct ScopedComponentHandle {
    value: Arc<dyn Any + Send + Sync>,
}

impl ScopedComponentHandle {
    /// Wraps one process-local component value.
    #[must_use]
    pub fn new<T>(value: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            value: Arc::new(value),
        }
    }

    /// Borrows the component when its concrete type is `T`.
    #[must_use]
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Any + Send + Sync,
    {
        self.value.downcast_ref::<T>()
    }

    #[cfg(test)]
    fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.value, &other.value)
    }
}

impl fmt::Debug for ScopedComponentHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopedComponentHandle")
            .finish_non_exhaustive()
    }
}

/// Value-redacted application factory failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedFactoryError;

impl fmt::Display for ScopedFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("scoped component factory failed")
    }
}

impl std::error::Error for ScopedFactoryError {}

/// Value-redacted application cleanup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedCleanupError;

impl fmt::Display for ScopedCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("scoped component cleanup failed")
    }
}

impl std::error::Error for ScopedCleanupError {}

/// Borrowed, process-local inputs supplied to one scoped component factory.
pub struct ScopedFactoryContext<'a> {
    inputs: &'a BTreeMap<ParameterName, ParameterValue>,
    dependencies: &'a BTreeMap<ScopedComponentId, ScopedComponentHandle>,
}

impl<'a> ScopedFactoryContext<'a> {
    pub(crate) const fn new(
        inputs: &'a BTreeMap<ParameterName, ParameterValue>,
        dependencies: &'a BTreeMap<ScopedComponentId, ScopedComponentHandle>,
    ) -> Self {
        Self {
            inputs,
            dependencies,
        }
    }

    /// Borrows the already-resolved late-bound inputs.
    #[must_use]
    pub const fn inputs(&self) -> &'a BTreeMap<ParameterName, ParameterValue> {
        self.inputs
    }

    /// Borrows one already-constructed declared dependency.
    #[must_use]
    pub fn dependency(&self, id: &ScopedComponentId) -> Option<&ScopedComponentHandle> {
        self.dependencies.get(id)
    }
}

/// Application-owned factory and cleanup contract for one live scoped component.
///
/// Factories receive only explicit resolved inputs and already-constructed
/// dependency handles; the framework performs no ambient lookup.
pub trait ScopedComponentFactory: Send + Sync {
    /// Constructs one attempt-local component instance.
    fn create<'a>(
        &'a self,
        context: ScopedFactoryContext<'a>,
    ) -> BoxFuture<'a, Result<ScopedComponentHandle, ScopedFactoryError>>;

    /// Releases one successfully constructed component.
    ///
    /// Cleanup is invoked at most once by the owning live scope.
    fn cleanup(
        &self,
        component: ScopedComponentHandle,
    ) -> BoxFuture<'_, Result<(), ScopedCleanupError>>;
}

/// Explicit application registration for one compiled scoped component.
///
/// Factory kind and revision are checked against the compiled definition
/// before durable launch. Dependencies are process-local assembly edges.
#[derive(Clone)]
pub struct ScopedComponentRegistration {
    scope: ScopeKind,
    id: ScopedComponentId,
    factory_kind: ScopeFactoryKind,
    factory_revision: ComponentRevision,
    dependencies: Vec<ScopedComponentId>,
    factory: Arc<dyn ScopedComponentFactory>,
}

impl ScopedComponentRegistration {
    /// Creates one explicit factory registration.
    ///
    /// # Errors
    ///
    /// Rejects duplicate dependency identifiers.
    pub fn new(
        scope: ScopeKind,
        id: ScopedComponentId,
        factory_kind: ScopeFactoryKind,
        factory_revision: ComponentRevision,
        mut dependencies: Vec<ScopedComponentId>,
        factory: Arc<dyn ScopedComponentFactory>,
    ) -> Result<Self, ScopeRegistrationError> {
        dependencies.sort();
        if dependencies.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ScopeRegistrationError::DuplicateDependency);
        }
        Ok(Self {
            scope,
            id,
            factory_kind,
            factory_revision,
            dependencies,
            factory,
        })
    }

    /// Returns the attempt-local scope kind.
    #[must_use]
    pub const fn scope(&self) -> ScopeKind {
        self.scope
    }

    /// Borrows the compiled logical component identifier.
    #[must_use]
    pub const fn id(&self) -> &ScopedComponentId {
        &self.id
    }

    /// Borrows the application factory kind.
    #[must_use]
    pub const fn factory_kind(&self) -> &ScopeFactoryKind {
        &self.factory_kind
    }

    /// Borrows the restart-relevant factory revision.
    #[must_use]
    pub const fn factory_revision(&self) -> &ComponentRevision {
        &self.factory_revision
    }

    /// Borrows process-local dependency identifiers in canonical order.
    #[must_use]
    pub fn dependencies(&self) -> &[ScopedComponentId] {
        &self.dependencies
    }
}

impl fmt::Debug for ScopedComponentRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopedComponentRegistration")
            .field("scope", &self.scope)
            .field("id", &self.id)
            .field("factory_kind", &self.factory_kind)
            .field("factory_revision", &self.factory_revision)
            .field("dependencies", &self.dependencies)
            .finish_non_exhaustive()
    }
}

/// Stable validation failure for a scoped component registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScopeRegistrationError {
    /// The registration names one dependency more than once.
    DuplicateDependency,
}

impl fmt::Display for ScopeRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateDependency => formatter.write_str(
                "scoped component registration contains a duplicate dependency",
            ),
        }
    }
}

impl std::error::Error for ScopeRegistrationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeBuildFailureKind {
    TooManyComponents,
    DuplicateComponent,
    WrongScope,
    MissingDependency,
    DependencyCycle,
    DependencyDepthExceeded,
    MissingInputs,
    FactoryRejected,
    FactoryPanicked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScopeBuildFailure {
    kind: ScopeBuildFailureKind,
    component: Option<ScopedComponentId>,
    cleanup_failures: usize,
}

impl ScopeBuildFailure {
    fn new(kind: ScopeBuildFailureKind, component: Option<ScopedComponentId>) -> Self {
        Self {
            kind,
            component,
            cleanup_failures: 0,
        }
    }

    pub(crate) const fn kind(&self) -> ScopeBuildFailureKind {
        self.kind
    }

    pub(crate) const fn cleanup_failures(&self) -> usize {
        self.cleanup_failures
    }

    pub(crate) const fn component(&self) -> Option<&ScopedComponentId> {
        self.component.as_ref()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScopeCleanupReport {
    failures: Vec<ScopedComponentId>,
}

impl ScopeCleanupReport {
    pub(crate) fn failures(&self) -> &[ScopedComponentId] {
        &self.failures
    }

    pub(crate) const fn is_clean(&self) -> bool {
        self.failures.is_empty()
    }
}

struct LiveEntry {
    handle: ScopedComponentHandle,
    factory: Arc<dyn ScopedComponentFactory>,
}

pub(crate) struct LiveScope {
    scope: ScopeKind,
    entries: BTreeMap<ScopedComponentId, LiveEntry>,
    construction_order: Vec<ScopedComponentId>,
    closed: bool,
}

impl fmt::Debug for LiveScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveScope")
            .field("scope", &self.scope)
            .field("component_count", &self.entries.len())
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl LiveScope {
    pub(crate) async fn build(
        scope: ScopeKind,
        registrations: Vec<ScopedComponentRegistration>,
        inputs: &BTreeMap<ScopedComponentId, BTreeMap<ParameterName, ParameterValue>>,
    ) -> Result<Self, ScopeBuildFailure> {
        let registrations = validate_graph(scope, registrations)?;
        let mut builder = ScopeBuilder {
            scope,
            registrations,
            inputs,
            entries: BTreeMap::new(),
            order: Vec::new(),
        };

        let ids = builder.registrations.keys().cloned().collect::<Vec<_>>();
        for id in ids {
            if builder.entries.contains_key(&id) {
                continue;
            }
            if let Err(mut failure) = builder.construct(id).await {
                let report = builder.cleanup_all().await;
                failure.cleanup_failures = report.failures.len();
                return Err(failure);
            }
        }

        Ok(Self {
            scope,
            entries: builder.entries,
            construction_order: builder.order,
            closed: false,
        })
    }

    pub(crate) const fn scope(&self) -> ScopeKind {
        self.scope
    }

    pub(crate) fn component(&self, id: &ScopedComponentId) -> Option<&ScopedComponentHandle> {
        self.entries.get(id).map(|entry| &entry.handle)
    }

    pub(crate) async fn close(&mut self) -> ScopeCleanupReport {
        if self.closed {
            return ScopeCleanupReport::default();
        }
        self.closed = true;

        let mut failures = Vec::new();
        while let Some(id) = self.construction_order.pop() {
            let Some(entry) = self.entries.remove(&id) else {
                continue;
            };
            if cleanup_entry(entry).await.is_err() {
                failures.push(id);
            }
        }
        ScopeCleanupReport { failures }
    }
}

struct ScopeBuilder<'a> {
    scope: ScopeKind,
    registrations: BTreeMap<ScopedComponentId, ScopedComponentRegistration>,
    inputs: &'a BTreeMap<ScopedComponentId, BTreeMap<ParameterName, ParameterValue>>,
    entries: BTreeMap<ScopedComponentId, LiveEntry>,
    order: Vec<ScopedComponentId>,
}

impl ScopeBuilder<'_> {
    fn construct(
        &mut self,
        id: ScopedComponentId,
    ) -> BoxFuture<'_, Result<ScopedComponentHandle, ScopeBuildFailure>> {
        Box::pin(async move {
            if let Some(entry) = self.entries.get(&id) {
                return Ok(entry.handle.clone());
            }

            let registration = self.registrations.get(&id).cloned().ok_or_else(|| {
                ScopeBuildFailure::new(ScopeBuildFailureKind::MissingDependency, Some(id.clone()))
            })?;
            debug_assert_eq!(registration.scope(), self.scope);

            let mut dependencies = BTreeMap::new();
            for dependency_id in registration.dependencies() {
                let dependency = self.construct(dependency_id.clone()).await?;
                dependencies.insert(dependency_id.clone(), dependency);
            }

            let inputs = self.inputs.get(&id).ok_or_else(|| {
                ScopeBuildFailure::new(ScopeBuildFailureKind::MissingInputs, Some(id.clone()))
            })?;
            let context = ScopedFactoryContext::new(inputs, &dependencies);
            let future = catch_unwind(AssertUnwindSafe(|| registration.factory.create(context)))
                .map_err(|_| {
                    ScopeBuildFailure::new(ScopeBuildFailureKind::FactoryPanicked, Some(id.clone()))
                })?;
            let handle = match AssertUnwindSafe(future).catch_unwind().await {
                Ok(Ok(handle)) => handle,
                Ok(Err(_)) => {
                    return Err(ScopeBuildFailure::new(
                        ScopeBuildFailureKind::FactoryRejected,
                        Some(id.clone()),
                    ));
                }
                Err(_) => {
                    return Err(ScopeBuildFailure::new(
                        ScopeBuildFailureKind::FactoryPanicked,
                        Some(id.clone()),
                    ));
                }
            };

            self.entries.insert(
                id.clone(),
                LiveEntry {
                    handle: handle.clone(),
                    factory: registration.factory,
                },
            );
            self.order.push(id.clone());
            Ok(handle)
        })
    }

    async fn cleanup_all(&mut self) -> ScopeCleanupReport {
        let mut failures = Vec::new();
        while let Some(id) = self.order.pop() {
            let Some(entry) = self.entries.remove(&id) else {
                continue;
            };
            if cleanup_entry(entry).await.is_err() {
                failures.push(id);
            }
        }
        ScopeCleanupReport { failures }
    }
}

async fn cleanup_entry(entry: LiveEntry) -> Result<(), ScopedCleanupError> {
    let factory = Arc::clone(&entry.factory);
    let component = entry.handle;
    let future = catch_unwind(AssertUnwindSafe(|| factory.cleanup(component)))
        .map_err(|_| ScopedCleanupError)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(result) => result,
        Err(_) => Err(ScopedCleanupError),
    }
}

fn validate_graph(
    scope: ScopeKind,
    registrations: Vec<ScopedComponentRegistration>,
) -> Result<BTreeMap<ScopedComponentId, ScopedComponentRegistration>, ScopeBuildFailure> {
    if registrations.len() > MAX_SCOPED_COMPONENTS {
        return Err(ScopeBuildFailure::new(
            ScopeBuildFailureKind::TooManyComponents,
            None,
        ));
    }

    let mut by_id = BTreeMap::new();
    for registration in registrations {
        if registration.scope() != scope {
            return Err(ScopeBuildFailure::new(
                ScopeBuildFailureKind::WrongScope,
                Some(registration.id().clone()),
            ));
        }
        let id = registration.id().clone();
        if by_id.insert(id.clone(), registration).is_some() {
            return Err(ScopeBuildFailure::new(
                ScopeBuildFailureKind::DuplicateComponent,
                Some(id),
            ));
        }
    }

    for registration in by_id.values() {
        for dependency in registration.dependencies() {
            if !by_id.contains_key(dependency) {
                return Err(ScopeBuildFailure::new(
                    ScopeBuildFailureKind::MissingDependency,
                    Some(registration.id().clone()),
                ));
            }
        }
    }

    let mut depths = BTreeMap::new();
    let mut stack = Vec::new();
    for id in by_id.keys() {
        validate_node(id, &by_id, &mut depths, &mut stack)?;
    }
    Ok(by_id)
}

fn validate_node(
    id: &ScopedComponentId,
    registrations: &BTreeMap<ScopedComponentId, ScopedComponentRegistration>,
    depths: &mut BTreeMap<ScopedComponentId, usize>,
    stack: &mut Vec<ScopedComponentId>,
) -> Result<usize, ScopeBuildFailure> {
    if let Some(depth) = depths.get(id) {
        return Ok(*depth);
    }
    if stack.iter().any(|active| active == id) {
        return Err(ScopeBuildFailure::new(
            ScopeBuildFailureKind::DependencyCycle,
            Some(id.clone()),
        ));
    }

    stack.push(id.clone());
    let registration = registrations.get(id).ok_or_else(|| {
        ScopeBuildFailure::new(ScopeBuildFailureKind::MissingDependency, Some(id.clone()))
    })?;
    let mut depth = 1_usize;
    for dependency in registration.dependencies() {
        let dependency_depth = validate_node(dependency, registrations, depths, stack)?;
        depth = depth.max(dependency_depth.saturating_add(1));
    }
    stack.pop();

    if depth > MAX_SCOPED_DEPENDENCY_DEPTH {
        return Err(ScopeBuildFailure::new(
            ScopeBuildFailureKind::DependencyDepthExceeded,
            Some(id.clone()),
        ));
    }

    depths.insert(id.clone(), depth);
    Ok(depth)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Clone, Copy)]
    enum CreateMode {
        Ok,
        Error,
        Panic,
    }

    #[derive(Clone, Copy)]
    enum CleanupMode {
        Ok,
        Error,
        Panic,
    }

    struct Factory {
        label: &'static str,
        creates: Arc<AtomicUsize>,
        cleanups: Arc<AtomicUsize>,
        events: Arc<Mutex<Vec<String>>>,
        create_mode: CreateMode,
        cleanup_mode: CleanupMode,
    }

    impl ScopedComponentFactory for Factory {
        fn create<'a>(
            &'a self,
            context: ScopedFactoryContext<'a>,
        ) -> BoxFuture<'a, Result<ScopedComponentHandle, ScopedFactoryError>> {
            Box::pin(async move {
                self.creates.fetch_add(1, Ordering::SeqCst);
                self.events
                    .lock()
                    .expect("events")
                    .push(format!("create:{}", self.label));
                let _ = context.inputs();
                match self.create_mode {
                    CreateMode::Ok => Ok(ScopedComponentHandle::new(self.label.to_string())),
                    CreateMode::Error => Err(ScopedFactoryError),
                    CreateMode::Panic => panic!("factory panic"),
                }
            })
        }

        fn cleanup(
            &self,
            component: ScopedComponentHandle,
        ) -> BoxFuture<'_, Result<(), ScopedCleanupError>> {
            Box::pin(async move {
                self.cleanups.fetch_add(1, Ordering::SeqCst);
                let value = component
                    .downcast_ref::<String>()
                    .expect("component type")
                    .clone();
                self.events
                    .lock()
                    .expect("events")
                    .push(format!("cleanup:{value}"));
                match self.cleanup_mode {
                    CleanupMode::Ok => Ok(()),
                    CleanupMode::Error => Err(ScopedCleanupError),
                    CleanupMode::Panic => panic!("cleanup panic"),
                }
            })
        }
    }

    fn id(value: &str) -> ScopedComponentId {
        ScopedComponentId::new(value).expect("component id")
    }

    fn registration(
        label: &'static str,
        dependencies: &[&str],
        events: Arc<Mutex<Vec<String>>>,
        create_mode: CreateMode,
        cleanup_mode: CleanupMode,
        creates: Arc<AtomicUsize>,
        cleanups: Arc<AtomicUsize>,
    ) -> ScopedComponentRegistration {
        ScopedComponentRegistration::new(
            ScopeKind::Step,
            id(label),
            ScopeFactoryKind::new("test-factory").expect("factory kind"),
            ComponentRevision::new("v1").expect("revision"),
            dependencies.iter().map(|value| id(value)).collect(),
            Arc::new(Factory {
                label,
                creates,
                cleanups,
                events,
                create_mode,
                cleanup_mode,
            }),
        )
        .expect("registration")
    }

    fn inputs(
        ids: &[&str],
    ) -> BTreeMap<ScopedComponentId, BTreeMap<ParameterName, ParameterValue>> {
        ids.iter()
            .map(|value| (id(value), BTreeMap::new()))
            .collect()
    }

    #[tokio::test]
    async fn dependencies_construct_once_and_cleanup_in_reverse_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let creates = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let regs = vec![
            registration(
                "a",
                &[],
                Arc::clone(&events),
                CreateMode::Ok,
                CleanupMode::Ok,
                Arc::clone(&creates),
                Arc::clone(&cleanups),
            ),
            registration(
                "b",
                &["a"],
                Arc::clone(&events),
                CreateMode::Ok,
                CleanupMode::Ok,
                Arc::clone(&creates),
                Arc::clone(&cleanups),
            ),
            registration(
                "c",
                &["a", "b"],
                Arc::clone(&events),
                CreateMode::Ok,
                CleanupMode::Ok,
                Arc::clone(&creates),
                Arc::clone(&cleanups),
            ),
        ];
        let mut scope = LiveScope::build(ScopeKind::Step, regs, &inputs(&["a", "b", "c"]))
            .await
            .expect("scope");

        assert_eq!(creates.load(Ordering::SeqCst), 3);
        let first = scope.component(&id("a")).expect("a").clone();
        let second = scope.component(&id("a")).expect("a").clone();
        assert!(first.ptr_eq(&second));

        let report = scope.close().await;
        assert!(report.is_clean());
        assert_eq!(cleanups.load(Ordering::SeqCst), 3);
        assert_eq!(
            *events.lock().expect("events"),
            vec![
                "create:a",
                "create:b",
                "create:c",
                "cleanup:c",
                "cleanup:b",
                "cleanup:a"
            ]
        );

        assert!(scope.close().await.is_clean());
        assert_eq!(cleanups.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn construction_failure_unwinds_all_successful_components_and_keeps_primary() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let creates = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let regs = vec![
            registration(
                "a",
                &[],
                Arc::clone(&events),
                CreateMode::Ok,
                CleanupMode::Error,
                Arc::clone(&creates),
                Arc::clone(&cleanups),
            ),
            registration(
                "b",
                &["a"],
                Arc::clone(&events),
                CreateMode::Error,
                CleanupMode::Ok,
                Arc::clone(&creates),
                Arc::clone(&cleanups),
            ),
        ];
        let failure = LiveScope::build(ScopeKind::Step, regs, &inputs(&["a", "b"]))
            .await
            .expect_err("must fail");

        assert_eq!(failure.kind(), ScopeBuildFailureKind::FactoryRejected);
        assert_eq!(failure.component(), Some(&id("b")));
        assert_eq!(failure.cleanup_failures(), 1);
        assert_eq!(
            *events.lock().expect("events"),
            vec!["create:a", "create:b", "cleanup:a"]
        );
    }

    #[tokio::test]
    async fn factory_and_cleanup_panics_are_contained() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let creates = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let regs = vec![
            registration(
                "a",
                &[],
                Arc::clone(&events),
                CreateMode::Ok,
                CleanupMode::Panic,
                Arc::clone(&creates),
                Arc::clone(&cleanups),
            ),
            registration(
                "b",
                &["a"],
                Arc::clone(&events),
                CreateMode::Panic,
                CleanupMode::Ok,
                Arc::clone(&creates),
                Arc::clone(&cleanups),
            ),
        ];
        let failure = LiveScope::build(ScopeKind::Step, regs, &inputs(&["a", "b"]))
            .await
            .expect_err("must fail");

        assert_eq!(failure.kind(), ScopeBuildFailureKind::FactoryPanicked);
        assert_eq!(failure.cleanup_failures(), 1);
        assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn graph_rejects_duplicate_missing_cycle_depth_and_capacity_before_construction() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let creates = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));

        let duplicate = registration(
            "dup",
            &[],
            Arc::clone(&events),
            CreateMode::Ok,
            CleanupMode::Ok,
            Arc::clone(&creates),
            Arc::clone(&cleanups),
        );
        assert_eq!(
            validate_graph(ScopeKind::Step, vec![duplicate.clone(), duplicate])
                .expect_err("duplicate")
                .kind(),
            ScopeBuildFailureKind::DuplicateComponent
        );

        let missing = registration(
            "missing-owner",
            &["not-registered"],
            Arc::clone(&events),
            CreateMode::Ok,
            CleanupMode::Ok,
            Arc::clone(&creates),
            Arc::clone(&cleanups),
        );
        assert_eq!(
            validate_graph(ScopeKind::Step, vec![missing])
                .expect_err("missing dependency")
                .kind(),
            ScopeBuildFailureKind::MissingDependency
        );

        let left = registration(
            "left",
            &["right"],
            Arc::clone(&events),
            CreateMode::Ok,
            CleanupMode::Ok,
            Arc::clone(&creates),
            Arc::clone(&cleanups),
        );
        let right = registration(
            "right",
            &["left"],
            Arc::clone(&events),
            CreateMode::Ok,
            CleanupMode::Ok,
            Arc::clone(&creates),
            Arc::clone(&cleanups),
        );
        assert_eq!(
            validate_graph(ScopeKind::Step, vec![left, right])
                .expect_err("cycle")
                .kind(),
            ScopeBuildFailureKind::DependencyCycle
        );

        let depth_regs = (0..=MAX_SCOPED_DEPENDENCY_DEPTH)
            .map(|index| {
                let label = format!("depth-{index}");
                let dependency = (index > 0).then(|| format!("depth-{}", index - 1));
                ScopedComponentRegistration::new(
                    ScopeKind::Step,
                    id(&label),
                    ScopeFactoryKind::new("test-factory").expect("factory kind"),
                    ComponentRevision::new("v1").expect("revision"),
                    dependency.iter().map(|value| id(value)).collect(),
                    Arc::new(Factory {
                        label: "depth",
                        creates: Arc::clone(&creates),
                        cleanups: Arc::clone(&cleanups),
                        events: Arc::clone(&events),
                        create_mode: CreateMode::Ok,
                        cleanup_mode: CleanupMode::Ok,
                    }),
                )
                .expect("registration")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            validate_graph(ScopeKind::Step, depth_regs)
                .expect_err("depth 33")
                .kind(),
            ScopeBuildFailureKind::DependencyDepthExceeded
        );

        let capacity = (0..=MAX_SCOPED_COMPONENTS)
            .map(|index| {
                ScopedComponentRegistration::new(
                    ScopeKind::Step,
                    id(&format!("component-{index}")),
                    ScopeFactoryKind::new("test-factory").expect("factory kind"),
                    ComponentRevision::new("v1").expect("revision"),
                    Vec::new(),
                    Arc::new(Factory {
                        label: "capacity",
                        creates: Arc::clone(&creates),
                        cleanups: Arc::clone(&cleanups),
                        events: Arc::clone(&events),
                        create_mode: CreateMode::Ok,
                        cleanup_mode: CleanupMode::Ok,
                    }),
                )
                .expect("registration")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            validate_graph(ScopeKind::Step, capacity)
                .expect_err("capacity 257")
                .kind(),
            ScopeBuildFailureKind::TooManyComponents
        );

        assert_eq!(creates.load(Ordering::SeqCst), 0);
    }
}
