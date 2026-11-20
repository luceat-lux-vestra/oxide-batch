//! Portable Gate-B durable-effect semantic and identity contracts.
//!
//! This module contains only driver-neutral, persistence-neutral values. It does
//! not serialize manifest format 5, persist effect state, open transactions, or
//! declare repository support. Those layers consume these contracts later.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU64};

use crate::{MAX_NODES, MAX_TRANSITIONS};

/// Gate-B semantic contract version accepted by ADR-0015.
pub const GATE_B_CONTRACT_VERSION: u16 = 1;

/// Maximum UTF-8 byte length of a Gate-B logical identifier.
///
/// ADR-0015 intentionally uses the same 1-128 byte discipline as existing
/// definition tokens. The bound is repeated here because Gate-B types are
/// deliberately independent from legacy manifest encoding.
pub const MAX_GATE_B_IDENTIFIER_BYTES: usize = 128;

/// Local atomicity required by a Gate-B definition.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum GateBLocalAtomicity {
    /// No same-resource atomicity requirement.
    #[default]
    None,
    /// Business mutation and protected framework progress share one resource commit.
    AtomicSameResource,
}

/// Effect-journal requirement.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum GateBEffectJournal {
    /// No durable effect journal is required.
    #[default]
    NotRequired,
    /// Durable effect-journal semantics are required.
    Required,
}

/// Outbound durable-obligation requirement.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum GateBOutboundObligation {
    /// No durable outbound obligation is selected.
    #[default]
    None,
    /// The authorizing mutation must atomically create an Outbox obligation.
    Outbox,
}

/// Inbound protection requirement.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum GateBInboundProtection {
    /// No inbound deduplication protection is selected.
    #[default]
    None,
    /// Stable inbound identity must be protected by InboxDedup semantics.
    InboxDedup,
}

/// Remote ambiguity and replay requirement.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum GateBAmbiguityReplay {
    /// No remote ambiguity/replay contract is selected.
    #[default]
    None,
    /// No default automatic replay after possible remote exposure.
    BestEffort,
    /// Replay is permitted even though the remote side may observe duplicates.
    AtLeastOnce,
    /// Replay requires the same certified request/provider-key identity.
    IdempotentExternalEffect,
}

/// A non-zero bounded Gate-B contract/profile version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GateBContractVersion(NonZeroU16);

impl GateBContractVersion {
    /// Constructs a non-zero version.
    #[must_use]
    pub const fn new(value: u16) -> Option<Self> {
        match NonZeroU16::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the integer version.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// A positive definition-level validity floor expressed in milliseconds.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GateBMinimumValidityMs(NonZeroU64);

impl GateBMinimumValidityMs {
    /// Constructs a positive millisecond floor.
    #[must_use]
    pub const fn new(milliseconds: u64) -> Option<Self> {
        match NonZeroU64::new(milliseconds) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the positive millisecond value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// A bounded portable semantic-contract identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GateBContractId(String);

impl GateBContractId {
    /// Validates and constructs one portable contract identifier.
    ///
    /// # Errors
    ///
    /// Rejects empty identifiers, values longer than 128 UTF-8 bytes,
    /// surrounding whitespace, and control characters.
    pub fn new(value: impl Into<String>) -> Result<Self, GateBContractError> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    /// Borrows the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A stable logical effect namespace.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectScopeId(String);

impl EffectScopeId {
    /// Validates and constructs one effect scope identifier.
    ///
    /// # Errors
    ///
    /// Rejects identifiers outside the Gate-B bounded token discipline.
    pub fn new(value: impl Into<String>) -> Result<Self, GateBContractError> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    /// Borrows the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A stable logical effect slot inside one effect scope.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectSlotId(String);

impl EffectSlotId {
    /// Validates and constructs one effect slot identifier.
    ///
    /// # Errors
    ///
    /// Rejects identifiers outside the Gate-B bounded token discipline.
    pub fn new(value: impl Into<String>) -> Result<Self, GateBContractError> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    /// Borrows the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque replay-stable identity of one logical external effect.
///
/// Construction is intentionally not public in this slice. A later derivation
/// implementation must produce this value from the accepted versioned,
/// domain-separated derivation contract; callers cannot manufacture one from a
/// transient attempt, random ID, wall clock, execution ID, or payload hash.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectId([u8; 32]);

impl EffectId {
    /// Returns the opaque fixed-size identity bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for EffectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EffectId(<opaque>)")
    }
}

/// One portable semantic contract ID/version pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBSemanticContract {
    id: GateBContractId,
    version: GateBContractVersion,
}

impl GateBSemanticContract {
    /// Constructs a portable semantic contract from validated values.
    #[must_use]
    pub const fn new(id: GateBContractId, version: GateBContractVersion) -> Self {
        Self { id, version }
    }

    /// Borrows the portable contract identifier.
    #[must_use]
    pub const fn id(&self) -> &GateBContractId {
        &self.id
    }

    /// Returns the non-zero semantic contract version.
    #[must_use]
    pub const fn version(&self) -> GateBContractVersion {
        self.version
    }
}

/// Broker-native transaction requirement, orthogonal to replay behavior.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum GateBNativeMessageTransaction {
    /// No broker-native transaction scope is required.
    #[default]
    None,
    /// A named portable broker-native semantic contract is required.
    Required(GateBSemanticContract),
}

/// Six orthogonal Gate-B requirement dimensions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBRequirements {
    local_atomicity: GateBLocalAtomicity,
    effect_journal: GateBEffectJournal,
    outbound_obligation: GateBOutboundObligation,
    inbound_protection: GateBInboundProtection,
    ambiguity_replay: GateBAmbiguityReplay,
    native_message_transaction: GateBNativeMessageTransaction,
}

impl GateBRequirements {
    /// Constructs one normalized Gate-B requirement set.
    ///
    /// # Errors
    ///
    /// Rejects the explicit version-1 contradiction where an idempotent
    /// external effect is selected without an effect journal.
    pub fn new(
        local_atomicity: GateBLocalAtomicity,
        effect_journal: GateBEffectJournal,
        outbound_obligation: GateBOutboundObligation,
        inbound_protection: GateBInboundProtection,
        ambiguity_replay: GateBAmbiguityReplay,
        native_message_transaction: GateBNativeMessageTransaction,
    ) -> Result<Self, GateBContractError> {
        if ambiguity_replay == GateBAmbiguityReplay::IdempotentExternalEffect
            && effect_journal != GateBEffectJournal::Required
        {
            return Err(GateBContractError::IdempotentExternalEffectRequiresJournal);
        }

        Ok(Self {
            local_atomicity,
            effect_journal,
            outbound_obligation,
            inbound_protection,
            ambiguity_replay,
            native_message_transaction,
        })
    }

    /// Returns the local-atomicity requirement.
    #[must_use]
    pub const fn local_atomicity(&self) -> GateBLocalAtomicity {
        self.local_atomicity
    }

    /// Returns the effect-journal requirement.
    #[must_use]
    pub const fn effect_journal(&self) -> GateBEffectJournal {
        self.effect_journal
    }

    /// Returns the outbound-obligation requirement.
    #[must_use]
    pub const fn outbound_obligation(&self) -> GateBOutboundObligation {
        self.outbound_obligation
    }

    /// Returns the inbound-protection requirement.
    #[must_use]
    pub const fn inbound_protection(&self) -> GateBInboundProtection {
        self.inbound_protection
    }

    /// Returns the remote ambiguity/replay requirement.
    #[must_use]
    pub const fn ambiguity_replay(&self) -> GateBAmbiguityReplay {
        self.ambiguity_replay
    }

    /// Borrows the broker-native transaction requirement.
    #[must_use]
    pub const fn native_message_transaction(&self) -> &GateBNativeMessageTransaction {
        &self.native_message_transaction
    }

    /// Returns whether ADR-0015 requires a portable remote semantic contract.
    #[must_use]
    pub fn requires_remote_contract(&self) -> bool {
        self.ambiguity_replay != GateBAmbiguityReplay::None
            || matches!(
                &self.native_message_transaction,
                GateBNativeMessageTransaction::Required(_)
            )
    }
}

/// One stable logical effect slot declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBEffectSlot {
    id: EffectSlotId,
    logical_destination_contract: Option<GateBContractId>,
}

impl GateBEffectSlot {
    /// Constructs one stable effect-slot declaration.
    #[must_use]
    pub const fn new(
        id: EffectSlotId,
        logical_destination_contract: Option<GateBContractId>,
    ) -> Self {
        Self {
            id,
            logical_destination_contract,
        }
    }

    /// Borrows the stable slot identifier.
    #[must_use]
    pub const fn id(&self) -> &EffectSlotId {
        &self.id
    }

    /// Borrows the logical-destination identity contract when selected.
    #[must_use]
    pub const fn logical_destination_contract(&self) -> Option<&GateBContractId> {
        self.logical_destination_contract.as_ref()
    }
}

/// One stable logical effect scope and its canonically ordered slots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBEffectScope {
    id: EffectScopeId,
    slots: Vec<GateBEffectSlot>,
}

impl GateBEffectScope {
    /// Constructs a scope and canonicalizes slot declaration order.
    ///
    /// # Errors
    ///
    /// Rejects duplicate slot identifiers.
    pub fn new(
        id: EffectScopeId,
        mut slots: Vec<GateBEffectSlot>,
    ) -> Result<Self, GateBContractError> {
        slots.sort_by(|left, right| left.id.cmp(&right.id));
        if slots.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err(GateBContractError::DuplicateEffectSlot);
        }
        Ok(Self { id, slots })
    }

    /// Borrows the stable effect-scope identifier.
    #[must_use]
    pub const fn id(&self) -> &EffectScopeId {
        &self.id
    }

    /// Iterates slots in canonical identifier order.
    pub fn slots(&self) -> impl ExactSizeIterator<Item = &GateBEffectSlot> {
        self.slots.iter()
    }
}

/// Static EffectId derivation and effect-scope/slot declaration contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBEffectIdentity {
    derivation_contract_version: GateBContractVersion,
    occurrence_key_contract_version: GateBContractVersion,
    request_identity_contract_version: GateBContractVersion,
    provider_key_binding_contract_version: Option<GateBContractVersion>,
    scopes: Vec<GateBEffectScope>,
}

impl GateBEffectIdentity {
    /// Constructs the static effect identity contract and canonicalizes scopes.
    ///
    /// # Errors
    ///
    /// Rejects duplicate scope identifiers and declarations exceeding the
    /// current reader capability ceilings. Provider-key selection consistency
    /// is validated by GateBDefinitionContract, where the remote requirement is
    /// also available.
    pub fn new(
        derivation_contract_version: GateBContractVersion,
        occurrence_key_contract_version: GateBContractVersion,
        request_identity_contract_version: GateBContractVersion,
        provider_key_binding_contract_version: Option<GateBContractVersion>,
        mut scopes: Vec<GateBEffectScope>,
    ) -> Result<Self, GateBContractError> {
        if scopes.len() > MAX_NODES {
            return Err(GateBContractError::TooManyEffectScopes { max: MAX_NODES });
        }

        scopes.sort_by(|left, right| left.id.cmp(&right.id));
        if scopes.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err(GateBContractError::DuplicateEffectScope);
        }

        let total_slots = scopes.iter().try_fold(0_usize, |total, scope| {
            total
                .checked_add(scope.slots.len())
                .ok_or(GateBContractError::TooManyEffectSlots {
                    max: MAX_TRANSITIONS,
                })
        })?;
        if total_slots > MAX_TRANSITIONS {
            return Err(GateBContractError::TooManyEffectSlots {
                max: MAX_TRANSITIONS,
            });
        }

        Ok(Self {
            derivation_contract_version,
            occurrence_key_contract_version,
            request_identity_contract_version,
            provider_key_binding_contract_version,
            scopes,
        })
    }

    /// Returns the EffectId derivation contract version.
    #[must_use]
    pub const fn derivation_contract_version(&self) -> GateBContractVersion {
        self.derivation_contract_version
    }

    /// Returns the occurrence-key contract version.
    #[must_use]
    pub const fn occurrence_key_contract_version(&self) -> GateBContractVersion {
        self.occurrence_key_contract_version
    }

    /// Returns the canonical request-identity contract version.
    #[must_use]
    pub const fn request_identity_contract_version(&self) -> GateBContractVersion {
        self.request_identity_contract_version
    }

    /// Returns the provider-key binding contract version when selected.
    #[must_use]
    pub const fn provider_key_binding_contract_version(&self) -> Option<GateBContractVersion> {
        self.provider_key_binding_contract_version
    }

    /// Iterates effect scopes in canonical identifier order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &GateBEffectScope> {
        self.scopes.iter()
    }
}

/// Portable remote replay/acknowledgement/unknown-outcome semantic contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBRemoteContract {
    semantic_profile: GateBSemanticContract,
    acknowledgement: GateBSemanticContract,
    unknown_outcome: GateBSemanticContract,
    minimum_validity_ms: Option<GateBMinimumValidityMs>,
}

impl GateBRemoteContract {
    /// Constructs a portable remote contract without a minimum validity floor.
    #[must_use]
    pub const fn new(
        semantic_profile: GateBSemanticContract,
        acknowledgement: GateBSemanticContract,
        unknown_outcome: GateBSemanticContract,
    ) -> Self {
        Self {
            semantic_profile,
            acknowledgement,
            unknown_outcome,
            minimum_validity_ms: None,
        }
    }

    /// Selects the definition-level remote replay/idempotency validity floor.
    #[must_use]
    pub const fn with_minimum_validity(mut self, minimum: GateBMinimumValidityMs) -> Self {
        self.minimum_validity_ms = Some(minimum);
        self
    }

    /// Borrows the portable semantic profile identity.
    #[must_use]
    pub const fn semantic_profile(&self) -> &GateBSemanticContract {
        &self.semantic_profile
    }

    /// Borrows the acknowledgement interpretation contract.
    #[must_use]
    pub const fn acknowledgement(&self) -> &GateBSemanticContract {
        &self.acknowledgement
    }

    /// Borrows the unknown-outcome interpretation contract.
    #[must_use]
    pub const fn unknown_outcome(&self) -> &GateBSemanticContract {
        &self.unknown_outcome
    }

    /// Returns the optional definition-level remote validity floor.
    #[must_use]
    pub const fn minimum_validity_ms(&self) -> Option<GateBMinimumValidityMs> {
        self.minimum_validity_ms
    }
}

/// Complete static Gate-B definition contract before manifest projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateBDefinitionContract {
    requirements: GateBRequirements,
    effect_identity: GateBEffectIdentity,
    remote_contract: Option<GateBRemoteContract>,
    inbox_dedup_minimum_validity_ms: Option<GateBMinimumValidityMs>,
}

impl GateBDefinitionContract {
    /// Constructs one fail-closed Gate-B definition contract.
    ///
    /// This validates only semantics frozen by Gate-B/ADR-0015. Recognition of
    /// a concrete portable remote profile and its provider-key compatibility is
    /// a later negotiation concern; provider/deployment identity never enters
    /// this value.
    ///
    /// # Errors
    ///
    /// Rejects missing or forbidden remote contracts, provider-key binding
    /// inconsistencies, and an InboxDedup floor without InboxDedup protection.
    pub fn new(
        requirements: GateBRequirements,
        effect_identity: GateBEffectIdentity,
        remote_contract: Option<GateBRemoteContract>,
        inbox_dedup_minimum_validity_ms: Option<GateBMinimumValidityMs>,
    ) -> Result<Self, GateBContractError> {
        if inbox_dedup_minimum_validity_ms.is_some()
            && requirements.inbound_protection() != GateBInboundProtection::InboxDedup
        {
            return Err(GateBContractError::InboxDedupFloorRequiresInboxDedup);
        }

        if requirements.ambiguity_replay() == GateBAmbiguityReplay::IdempotentExternalEffect
            && effect_identity
                .provider_key_binding_contract_version()
                .is_none()
        {
            return Err(GateBContractError::ProviderKeyBindingRequired);
        }

        if effect_identity
            .provider_key_binding_contract_version()
            .is_some()
            && remote_contract.is_none()
        {
            return Err(GateBContractError::ProviderKeyBindingRequiresRemoteContract);
        }

        match (requirements.requires_remote_contract(), remote_contract.is_some()) {
            (true, false) => return Err(GateBContractError::RemoteContractRequired),
            (false, true) => return Err(GateBContractError::RemoteContractForbidden),
            _ => {}
        }

        Ok(Self {
            requirements,
            effect_identity,
            remote_contract,
            inbox_dedup_minimum_validity_ms,
        })
    }

    /// Returns the fixed Gate-B contract version.
    #[must_use]
    pub const fn contract_version(&self) -> u16 {
        GATE_B_CONTRACT_VERSION
    }

    /// Borrows the six normalized requirement dimensions.
    #[must_use]
    pub const fn requirements(&self) -> &GateBRequirements {
        &self.requirements
    }

    /// Borrows the static effect-identity declaration.
    #[must_use]
    pub const fn effect_identity(&self) -> &GateBEffectIdentity {
        &self.effect_identity
    }

    /// Borrows the portable remote contract when selected.
    #[must_use]
    pub const fn remote_contract(&self) -> Option<&GateBRemoteContract> {
        self.remote_contract.as_ref()
    }

    /// Returns the definition-required InboxDedup floor when selected.
    #[must_use]
    pub const fn inbox_dedup_minimum_validity_ms(&self) -> Option<GateBMinimumValidityMs> {
        self.inbox_dedup_minimum_validity_ms
    }
}

/// A fail-closed Gate-B semantic contract construction error.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GateBContractError {
    /// A logical identifier was empty.
    EmptyIdentifier,
    /// A logical identifier exceeded the byte bound.
    IdentifierTooLong {
        /// Maximum accepted UTF-8 byte length.
        max_bytes: usize,
    },
    /// A logical identifier had leading or trailing whitespace.
    SurroundingWhitespace,
    /// A logical identifier contained a control character.
    ControlCharacter,
    /// The same EffectScopeId was declared more than once.
    DuplicateEffectScope,
    /// The same EffectSlotId was declared more than once inside one scope.
    DuplicateEffectSlot,
    /// The declaration exceeds the reader's effect-scope capability ceiling.
    TooManyEffectScopes {
        /// Maximum accepted scopes.
        max: usize,
    },
    /// The declaration exceeds the reader's total effect-slot capability ceiling.
    TooManyEffectSlots {
        /// Maximum accepted total slots.
        max: usize,
    },
    /// Idempotent external replay was selected without an EffectJournal.
    IdempotentExternalEffectRequiresJournal,
    /// Selected semantics require a portable remote contract.
    RemoteContractRequired,
    /// A remote contract was supplied although no remote semantic is selected.
    RemoteContractForbidden,
    /// Idempotent external replay requires a provider-key binding contract version.
    ProviderKeyBindingRequired,
    /// A provider-key binding version cannot exist without a remote contract.
    ProviderKeyBindingRequiresRemoteContract,
    /// An InboxDedup floor was supplied without InboxDedup protection.
    InboxDedupFloorRequiresInboxDedup,
}

impl fmt::Display for GateBContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyIdentifier => formatter.write_str("Gate-B identifier is empty"),
            Self::IdentifierTooLong { max_bytes } => {
                write!(formatter, "Gate-B identifier exceeds {max_bytes} UTF-8 bytes")
            }
            Self::SurroundingWhitespace => {
                formatter.write_str("Gate-B identifier has surrounding whitespace")
            }
            Self::ControlCharacter => {
                formatter.write_str("Gate-B identifier contains a control character")
            }
            Self::DuplicateEffectScope => {
                formatter.write_str("Gate-B effect scope identifier is duplicated")
            }
            Self::DuplicateEffectSlot => {
                formatter.write_str("Gate-B effect slot identifier is duplicated")
            }
            Self::TooManyEffectScopes { max } => {
                write!(formatter, "Gate-B definition exceeds {max} effect scopes")
            }
            Self::TooManyEffectSlots { max } => {
                write!(formatter, "Gate-B definition exceeds {max} total effect slots")
            }
            Self::IdempotentExternalEffectRequiresJournal => formatter.write_str(
                "idempotent external effects require a durable effect journal",
            ),
            Self::RemoteContractRequired => {
                formatter.write_str("selected Gate-B semantics require a remote contract")
            }
            Self::RemoteContractForbidden => {
                formatter.write_str("Gate-B remote contract is not selected by requirements")
            }
            Self::ProviderKeyBindingRequired => formatter.write_str(
                "idempotent external effects require a provider-key binding contract version",
            ),
            Self::ProviderKeyBindingRequiresRemoteContract => formatter.write_str(
                "provider-key binding contract version requires a remote contract",
            ),
            Self::InboxDedupFloorRequiresInboxDedup => formatter.write_str(
                "InboxDedup minimum validity requires InboxDedup protection",
            ),
        }
    }
}

impl Error for GateBContractError {}

fn validate_identifier(value: &str) -> Result<(), GateBContractError> {
    if value.is_empty() {
        return Err(GateBContractError::EmptyIdentifier);
    }
    if value.len() > MAX_GATE_B_IDENTIFIER_BYTES {
        return Err(GateBContractError::IdentifierTooLong {
            max_bytes: MAX_GATE_B_IDENTIFIER_BYTES,
        });
    }
    if value.trim() != value {
        return Err(GateBContractError::SurroundingWhitespace);
    }
    if value.chars().any(char::is_control) {
        return Err(GateBContractError::ControlCharacter);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "contract fixtures use statically valid values and assert the error cases separately"
    )]

    use super::*;

    fn version(value: u16) -> GateBContractVersion {
        GateBContractVersion::new(value).expect("test version is nonzero")
    }

    fn contract(id: &str) -> GateBSemanticContract {
        GateBSemanticContract::new(
            GateBContractId::new(id).expect("test contract id is valid"),
            version(1),
        )
    }

    fn remote() -> GateBRemoteContract {
        GateBRemoteContract::new(
            contract("remote-profile"),
            contract("ack"),
            contract("unknown-outcome"),
        )
    }

    fn identity(
        provider_key_binding: Option<GateBContractVersion>,
    ) -> Result<GateBEffectIdentity, GateBContractError> {
        GateBEffectIdentity::new(
            version(1),
            version(1),
            version(1),
            provider_key_binding,
            Vec::new(),
        )
    }

    #[test]
    fn orthogonal_requirements_allow_outbox_plus_best_effort() {
        let requirements = GateBRequirements::new(
            GateBLocalAtomicity::AtomicSameResource,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::Outbox,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::BestEffort,
            GateBNativeMessageTransaction::None,
        )
        .expect("composition is valid");

        let definition =
            GateBDefinitionContract::new(requirements, identity(None).unwrap(), Some(remote()), None)
                .expect("remote contract satisfies BestEffort");

        assert_eq!(
            definition.requirements().outbound_obligation(),
            GateBOutboundObligation::Outbox
        );
        assert_eq!(
            definition.requirements().ambiguity_replay(),
            GateBAmbiguityReplay::BestEffort
        );
    }

    #[test]
    fn idempotent_external_effect_requires_effect_journal() {
        assert_eq!(
            GateBRequirements::new(
                GateBLocalAtomicity::None,
                GateBEffectJournal::NotRequired,
                GateBOutboundObligation::None,
                GateBInboundProtection::None,
                GateBAmbiguityReplay::IdempotentExternalEffect,
                GateBNativeMessageTransaction::None,
            ),
            Err(GateBContractError::IdempotentExternalEffectRequiresJournal)
        );
    }

    #[test]
    fn idempotent_external_effect_requires_provider_key_binding_version() {
        let requirements = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::Required,
            GateBOutboundObligation::None,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::IdempotentExternalEffect,
            GateBNativeMessageTransaction::None,
        )
        .unwrap();

        assert_eq!(
            GateBDefinitionContract::new(requirements, identity(None).unwrap(), Some(remote()), None),
            Err(GateBContractError::ProviderKeyBindingRequired)
        );
    }

    #[test]
    fn provider_key_binding_requires_remote_contract() {
        let requirements = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::None,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::None,
            GateBNativeMessageTransaction::None,
        )
        .unwrap();

        assert_eq!(
            GateBDefinitionContract::new(
                requirements,
                identity(Some(version(7))).unwrap(),
                None,
                None,
            ),
            Err(GateBContractError::ProviderKeyBindingRequiresRemoteContract)
        );
    }

    #[test]
    fn remote_contract_presence_is_selected_by_remote_semantics() {
        let none = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::None,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::None,
            GateBNativeMessageTransaction::None,
        )
        .unwrap();
        assert_eq!(
            GateBDefinitionContract::new(
                none.clone(),
                identity(None).unwrap(),
                Some(remote()),
                None,
            ),
            Err(GateBContractError::RemoteContractForbidden)
        );

        let best_effort = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::None,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::BestEffort,
            GateBNativeMessageTransaction::None,
        )
        .unwrap();
        assert_eq!(
            GateBDefinitionContract::new(best_effort, identity(None).unwrap(), None, None),
            Err(GateBContractError::RemoteContractRequired)
        );

        GateBDefinitionContract::new(none, identity(None).unwrap(), None, None)
            .expect("no remote semantics means no remote contract");
    }

    #[test]
    fn native_message_transaction_requires_remote_contract_independently() {
        let native = GateBNativeMessageTransaction::Required(contract("broker-native"));
        let requirements = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::None,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::None,
            native,
        )
        .unwrap();

        assert_eq!(
            GateBDefinitionContract::new(requirements, identity(None).unwrap(), None, None),
            Err(GateBContractError::RemoteContractRequired)
        );
    }

    #[test]
    fn provider_key_binding_is_allowed_when_a_remote_profile_selects_it() {
        let requirements = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::None,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::AtLeastOnce,
            GateBNativeMessageTransaction::None,
        )
        .unwrap();

        let definition = GateBDefinitionContract::new(
            requirements,
            identity(Some(version(7))).unwrap(),
            Some(remote()),
            None,
        )
        .expect("a recognized remote profile may select provider-key binding");

        assert_eq!(
            definition
                .effect_identity()
                .provider_key_binding_contract_version(),
            Some(version(7))
        );
    }

    #[test]
    fn inbox_floor_requires_inbox_and_is_independent_from_remote_floor() {
        let floor = GateBMinimumValidityMs::new(60_000).unwrap();
        let remote_floor = GateBMinimumValidityMs::new(5_000).unwrap();

        let without_inbox = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::None,
            GateBInboundProtection::None,
            GateBAmbiguityReplay::None,
            GateBNativeMessageTransaction::None,
        )
        .unwrap();
        assert_eq!(
            GateBDefinitionContract::new(
                without_inbox,
                identity(None).unwrap(),
                None,
                Some(floor),
            ),
            Err(GateBContractError::InboxDedupFloorRequiresInboxDedup)
        );

        let with_both = GateBRequirements::new(
            GateBLocalAtomicity::None,
            GateBEffectJournal::NotRequired,
            GateBOutboundObligation::None,
            GateBInboundProtection::InboxDedup,
            GateBAmbiguityReplay::AtLeastOnce,
            GateBNativeMessageTransaction::None,
        )
        .unwrap();
        let definition = GateBDefinitionContract::new(
            with_both,
            identity(None).unwrap(),
            Some(remote().with_minimum_validity(remote_floor)),
            Some(floor),
        )
        .expect("remote and InboxDedup floors may coexist");

        assert_eq!(
            definition
                .remote_contract()
                .and_then(GateBRemoteContract::minimum_validity_ms),
            Some(remote_floor)
        );
        assert_eq!(definition.inbox_dedup_minimum_validity_ms(), Some(floor));
    }

    #[test]
    fn scope_and_slot_declarations_are_canonical_and_unique() {
        let slot_a = GateBEffectSlot::new(EffectSlotId::new("a").unwrap(), None);
        let slot_b = GateBEffectSlot::new(EffectSlotId::new("b").unwrap(), None);
        let scope_b =
            GateBEffectScope::new(EffectScopeId::new("b").unwrap(), vec![slot_b, slot_a]).unwrap();
        assert_eq!(
            scope_b
                .slots()
                .map(|slot| slot.id().as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        let scope_a = GateBEffectScope::new(EffectScopeId::new("a").unwrap(), Vec::new()).unwrap();
        let identity =
            GateBEffectIdentity::new(version(1), version(1), version(1), None, vec![scope_b, scope_a])
                .unwrap();
        assert_eq!(
            identity
                .scopes()
                .map(|scope| scope.id().as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        assert_eq!(
            GateBEffectScope::new(
                EffectScopeId::new("dup-scope").unwrap(),
                vec![
                    GateBEffectSlot::new(EffectSlotId::new("dup").unwrap(), None),
                    GateBEffectSlot::new(EffectSlotId::new("dup").unwrap(), None),
                ],
            ),
            Err(GateBContractError::DuplicateEffectSlot)
        );
    }

    #[test]
    fn identity_declaration_enforces_scope_and_total_slot_bounds() {
        let too_many_scopes = (0..=MAX_NODES)
            .map(|index| {
                GateBEffectScope::new(
                    EffectScopeId::new(format!("scope-{index}")).unwrap(),
                    Vec::new(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            GateBEffectIdentity::new(
                version(1),
                version(1),
                version(1),
                None,
                too_many_scopes,
            ),
            Err(GateBContractError::TooManyEffectScopes { max: MAX_NODES })
        );

        let slots = (0..=MAX_TRANSITIONS)
            .map(|index| {
                GateBEffectSlot::new(EffectSlotId::new(format!("slot-{index}")).unwrap(), None)
            })
            .collect::<Vec<_>>();
        let scope = GateBEffectScope::new(EffectScopeId::new("scope").unwrap(), slots).unwrap();
        assert_eq!(
            GateBEffectIdentity::new(version(1), version(1), version(1), None, vec![scope]),
            Err(GateBContractError::TooManyEffectSlots {
                max: MAX_TRANSITIONS
            })
        );
    }

    #[test]
    fn identifiers_versions_and_validity_floors_are_bounded() {
        assert_eq!(
            GateBContractId::new(""),
            Err(GateBContractError::EmptyIdentifier)
        );
        assert_eq!(
            EffectScopeId::new(" scope "),
            Err(GateBContractError::SurroundingWhitespace)
        );
        assert_eq!(
            EffectSlotId::new("slot\n"),
            Err(GateBContractError::ControlCharacter)
        );
        assert_eq!(
            GateBContractId::new("x".repeat(MAX_GATE_B_IDENTIFIER_BYTES + 1)),
            Err(GateBContractError::IdentifierTooLong {
                max_bytes: MAX_GATE_B_IDENTIFIER_BYTES
            })
        );
        assert!(GateBContractVersion::new(0).is_none());
        assert!(GateBMinimumValidityMs::new(0).is_none());
    }

    #[test]
    fn effect_id_debug_is_opaque() {
        let id = EffectId([0x5a; 32]);
        assert_eq!(format!("{id:?}"), "EffectId(<opaque>)");
        assert_eq!(id.as_bytes(), &[0x5a; 32]);
    }
}
