//! Typed service failures mapped to stable exit categories.
//!
//! Mapping lives in one module so that a category is decided by the failure's
//! type rather than by inspecting an error string. No mapping reads or forwards
//! a message produced by a database driver or by user component code.

use oxide_batch::{
    ExplorerError, LifecycleError, OperatorError, OperatorRejection, RepositoryError,
    RetentionError,
};

use crate::exit::ExitCategory;
use crate::output::Diagnostic;

/// Maps one repository failure to its exit category.
pub fn repository(error: &RepositoryError) -> ExitCategory {
    match error {
        RepositoryError::JobInstanceNotFound { .. }
        | RepositoryError::JobExecutionNotFound { .. }
        | RepositoryError::StepExecutionNotFound { .. }
        | RepositoryError::RestartStateNotFound { .. } => ExitCategory::TargetNotFound,
        RepositoryError::CommitOutcomeUnknown => ExitCategory::OutcomeUnknown,
        RepositoryError::ConcurrentModification
        | RepositoryError::RetentionPlanStale
        | RepositoryError::Lifecycle(LifecycleError::StaleVersion { .. }) => {
            ExitCategory::OptimisticConflict
        }
        RepositoryError::SchemaUninitialized
        | RepositoryError::MigrationRequired { .. }
        | RepositoryError::NewerSchema { .. }
        | RepositoryError::Unavailable => ExitCategory::RepositoryUnavailable,
        RepositoryError::Identifier(_) => ExitCategory::Internal,
        RepositoryError::Domain(_) | RepositoryError::IdentifierOutOfRange { .. } => {
            ExitCategory::Usage
        }
        _ => ExitCategory::GuardRejected,
    }
}

/// Returns the safe diagnostic for one repository failure.
///
/// The detail is a fixed phrase selected by the failure's type. A driver
/// message, SQL text, connection string, or user error text is never included.
pub fn repository_diagnostic(error: &RepositoryError) -> Diagnostic {
    let (code, detail) = match error {
        RepositoryError::SchemaUninitialized => (
            "SCHEMA_UNINITIALIZED",
            "the metadata schema is not initialized",
        ),
        RepositoryError::MigrationRequired { .. } => (
            "MIGRATION_REQUIRED",
            "the metadata schema requires migration before use",
        ),
        RepositoryError::NewerSchema { .. } => (
            "SCHEMA_NEWER",
            "the metadata schema is newer than this build supports",
        ),
        RepositoryError::Unavailable => ("REPOSITORY_UNAVAILABLE", "the repository is unavailable"),
        RepositoryError::CommitOutcomeUnknown => (
            "OUTCOME_UNKNOWN",
            "the durable outcome is undetermined; replay the same operation identifier",
        ),
        RepositoryError::UnsupportedCapability { .. } => (
            "UNSUPPORTED_CAPABILITY",
            "the adapter does not provide a capability this command requires",
        ),
        RepositoryError::ConcurrentModification => (
            "CONCURRENT_MODIFICATION",
            "another committed unit of work invalidated the observed snapshot",
        ),
        _ => ("REPOSITORY_REJECTED", "the repository rejected the action"),
    };
    Diagnostic::new(code, detail)
}

/// Maps one explorer failure to its exit category.
///
/// The rejected-query variants are listed explicitly rather than folded into
/// the fallback, so adding a variant to the service is a visible decision here
/// instead of silently inheriting a category.
#[allow(clippy::match_same_arms)]
pub fn explorer(error: &ExplorerError) -> ExitCategory {
    match error {
        ExplorerError::PageSizeOutOfRange { .. } => ExitCategory::ConfigurationInvalid,
        ExplorerError::AgeBoundTooSmall { .. } => ExitCategory::Usage,
        ExplorerError::Cursor(_)
        | ExplorerError::ResponseTooLarge { .. }
        | ExplorerError::UnsupportedCapability => ExitCategory::GuardRejected,
        ExplorerError::Timeout => ExitCategory::DeadlineExceeded,
        ExplorerError::Repository(error) => repository(error),
        _ => ExitCategory::GuardRejected,
    }
}

/// Returns the safe diagnostic for one explorer failure.
pub fn explorer_diagnostic(error: &ExplorerError) -> Diagnostic {
    match error {
        ExplorerError::PageSizeOutOfRange { .. } => Diagnostic::new(
            "PAGE_SIZE_OUT_OF_RANGE",
            "the page size is outside its bound",
        ),
        ExplorerError::AgeBoundTooSmall { minimum } => Diagnostic::new(
            "AGE_BOUND_TOO_SMALL",
            format!(
                "the age bound must be at least {} seconds",
                minimum.as_secs()
            ),
        ),
        ExplorerError::Cursor(_) => Diagnostic::new(
            "CURSOR_REJECTED",
            "the continuation token does not belong to this query",
        ),
        ExplorerError::ResponseTooLarge { limit } => Diagnostic::new(
            "RESPONSE_TOO_LARGE",
            format!("one row alone exceeds the {limit} byte response bound"),
        ),
        ExplorerError::UnsupportedCapability => Diagnostic::new(
            "UNSUPPORTED_CAPABILITY",
            "the adapter does not support bounded keyset pagination",
        ),
        ExplorerError::Timeout => Diagnostic::new(
            "STATEMENT_TIMEOUT",
            "the bounded query exceeded its statement timeout",
        ),
        ExplorerError::Repository(error) => repository_diagnostic(error),
        _ => Diagnostic::new("QUERY_REJECTED", "the bounded query was rejected"),
    }
}

/// Maps one operator failure to its exit category.
///
/// The conflict variant is listed explicitly rather than folded into the
/// fallback, so adding a variant to the service is a visible decision here.
#[allow(clippy::match_same_arms)]
pub fn operator(error: &OperatorError) -> ExitCategory {
    match error {
        OperatorError::OperationIdConflict { .. } => ExitCategory::GuardRejected,
        OperatorError::OperationOutcomeUnknown => ExitCategory::OutcomeUnknown,
        OperatorError::InvalidRecoveryRequest(_) => ExitCategory::Usage,
        OperatorError::Repository(error) => repository(error),
        _ => ExitCategory::GuardRejected,
    }
}

/// Returns the safe diagnostic for one operator failure.
pub fn operator_diagnostic(error: &OperatorError) -> Diagnostic {
    match error {
        OperatorError::OperationIdConflict { .. } => Diagnostic::new(
            "OPERATION_ID_CONFLICT",
            "the operation identifier was already recorded for a different request",
        ),
        OperatorError::OperationOutcomeUnknown => Diagnostic::new(
            "OUTCOME_UNKNOWN",
            "the durable outcome is undetermined; replay the same operation identifier",
        ),
        OperatorError::InvalidRecoveryRequest(_) => Diagnostic::new(
            "INVALID_RECOVERY_REQUEST",
            "the recovery arguments do not form a valid audited request",
        ),
        OperatorError::Repository(error) => repository_diagnostic(error),
        _ => Diagnostic::new("OPERATOR_REJECTED", "the operator action was rejected"),
    }
}

/// Maps one audited guard rejection to its exit category.
///
/// A rejection is a durable, audited outcome rather than an error, so the
/// category reports why the guard refused rather than that a call failed.
pub fn rejection(value: OperatorRejection) -> ExitCategory {
    match value {
        OperatorRejection::OptimisticConflict { .. } => ExitCategory::OptimisticConflict,
        OperatorRejection::ExecutionNotFound | OperatorRejection::InstanceNotFound => {
            ExitCategory::TargetNotFound
        }
        _ => ExitCategory::GuardRejected,
    }
}

/// Maps one retention failure to its exit category.
pub fn retention(error: &RetentionError) -> ExitCategory {
    match error {
        RetentionError::BatchBoundOutOfRange { .. }
        | RetentionError::AgeBoundTooSmall { .. }
        | RetentionError::NonTerminalStatus { .. }
        | RetentionError::EmptyStatusSet => ExitCategory::Usage,
        RetentionError::RetentionPlanStale => ExitCategory::OptimisticConflict,
        RetentionError::InstanceHeld { .. } | RetentionError::OperationIdConflict { .. } => {
            ExitCategory::GuardRejected
        }
        RetentionError::OperationOutcomeUnknown => ExitCategory::OutcomeUnknown,
        RetentionError::Repository(error) => repository(error),
        _ => ExitCategory::GuardRejected,
    }
}

/// Returns the safe diagnostic for one retention failure.
pub fn retention_diagnostic(error: &RetentionError) -> Diagnostic {
    match error {
        RetentionError::BatchBoundOutOfRange { .. } => Diagnostic::new(
            "BATCH_BOUND_OUT_OF_RANGE",
            "the purge batch bound is outside its accepted range",
        ),
        RetentionError::AgeBoundTooSmall { minimum } => Diagnostic::new(
            "AGE_BOUND_TOO_SMALL",
            format!(
                "the purge age bound must be at least {} seconds",
                minimum.as_secs()
            ),
        ),
        RetentionError::NonTerminalStatus { .. } => Diagnostic::new(
            "NON_TERMINAL_STATUS",
            "a purge may target only finished statuses",
        ),
        RetentionError::EmptyStatusSet => Diagnostic::new(
            "EMPTY_STATUS_SET",
            "a purge must target at least one status",
        ),
        RetentionError::RetentionPlanStale => Diagnostic::new(
            "PLAN_STALE",
            "a candidate changed after the plan was produced; nothing was deleted",
        ),
        RetentionError::InstanceHeld { .. } => Diagnostic::new(
            "INSTANCE_HELD",
            "the instance is held and can be neither planned nor purged",
        ),
        RetentionError::OperationIdConflict { .. } => Diagnostic::new(
            "OPERATION_ID_CONFLICT",
            "the operation identifier was already recorded for a different request",
        ),
        RetentionError::OperationOutcomeUnknown => Diagnostic::new(
            "OUTCOME_UNKNOWN",
            "the durable outcome is undetermined; replay the same operation identifier",
        ),
        RetentionError::Repository(error) => repository_diagnostic(error),
        _ => Diagnostic::new("RETENTION_REJECTED", "the retention action was rejected"),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use oxide_batch::{ExecutionVersion, ExplorerError, OperatorRejection, RepositoryError};

    use super::{explorer, rejection, repository, repository_diagnostic};
    use crate::exit::ExitCategory;

    #[test]
    fn an_unknown_commit_reports_the_unknown_category() {
        assert_eq!(
            repository(&RepositoryError::CommitOutcomeUnknown),
            ExitCategory::OutcomeUnknown
        );
    }

    #[test]
    fn an_unavailable_repository_reports_its_category() {
        assert_eq!(
            repository(&RepositoryError::Unavailable),
            ExitCategory::RepositoryUnavailable
        );
    }

    #[test]
    fn a_stale_version_rejection_reports_a_conflict() {
        assert_eq!(
            rejection(OperatorRejection::OptimisticConflict {
                current: ExecutionVersion::new(4)
            }),
            ExitCategory::OptimisticConflict
        );
    }

    #[test]
    fn a_missing_execution_reports_target_not_found() {
        assert_eq!(
            rejection(OperatorRejection::ExecutionNotFound),
            ExitCategory::TargetNotFound
        );
    }

    #[test]
    fn a_statement_timeout_reports_the_deadline_category() {
        assert_eq!(
            explorer(&ExplorerError::Timeout),
            ExitCategory::DeadlineExceeded
        );
    }

    #[test]
    fn diagnostics_carry_no_driver_text() {
        let diagnostic = repository_diagnostic(&RepositoryError::Unavailable);
        assert_eq!(diagnostic.code, "REPOSITORY_UNAVAILABLE");
        assert!(!diagnostic.detail.is_empty());
    }
}
