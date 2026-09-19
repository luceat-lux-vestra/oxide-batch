use std::collections::BTreeSet;

use oxide_batch_repository::{RepositoryError, ScopeResolutionProvenance, ScopeResolutionSource};
use serde_json::{Value, json};
use sqlx::types::Json;
use sqlx::{PgConnection, Row};

use crate::{
    ExecutionVersion, IdentifierKind, JobExecutionId, NodeId, ParameterName, ScopeFrameworkSource,
    ScopeKind, ScopedComponentId, StateSchemaId, StateSchemaVersion, StepExecutionId,
};

pub(crate) async fn load_scope_resolution_provenance(
    transaction: &mut PgConnection,
    scope: ScopeKind,
    component: &ScopedComponentId,
    owner_job_execution_id: JobExecutionId,
    owner_step_execution_id: Option<StepExecutionId>,
) -> Result<Vec<ScopeResolutionProvenance>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT scope_kind, component_id, input_name, owner_job_execution_id,          owner_step_execution_id, source_kind, source_parameter_name, source_node_id,          source_framework_field, source_job_execution_id, source_step_execution_id,          source_execution_version, source_schema, source_schema_version, source_path          FROM oxide_batch.ob_scope_resolution_provenance          WHERE scope_kind = $1 AND component_id = $2 AND owner_job_execution_id = $3            AND owner_step_execution_id IS NOT DISTINCT FROM $4          ORDER BY input_name",
    )
    .bind(scope.as_str())
    .bind(component.as_str())
    .bind(job_database_id(owner_job_execution_id)?)
    .bind(owner_step_execution_id.map(step_database_id).transpose()?)
    .fetch_all(transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;

    rows.iter().map(decode_provenance).collect()
}

pub(crate) async fn store_scope_resolution_provenance(
    transaction: &mut PgConnection,
    entries: &[ScopeResolutionProvenance],
) -> Result<(), RepositoryError> {
    if entries.len() > oxide_batch_core::MAX_LATE_BOUND_INPUTS {
        return Err(RepositoryError::ScopeResolutionStateCorrupt);
    }
    let Some(first) = entries.first() else {
        return Ok(());
    };
    let mut inputs = BTreeSet::new();
    for entry in entries {
        if entry.scope() != first.scope()
            || entry.component() != first.component()
            || entry.owner_job_execution_id() != first.owner_job_execution_id()
            || entry.owner_step_execution_id() != first.owner_step_execution_id()
            || !inputs.insert(entry.input().clone())
        {
            return Err(RepositoryError::ScopeResolutionStateCorrupt);
        }
    }
    for entry in entries {
        validate_source_instance(transaction, entry).await?;
        insert_provenance(transaction, entry).await?;
    }

    let persisted = load_scope_resolution_provenance(
        transaction,
        first.scope(),
        first.component(),
        first.owner_job_execution_id(),
        first.owner_step_execution_id(),
    )
    .await?;
    let mut expected = entries.to_vec();
    expected.sort_by(|left, right| left.input().cmp(right.input()));
    if persisted != expected {
        return Err(RepositoryError::ScopeResolutionStateCorrupt);
    }
    Ok(())
}

pub(crate) async fn has_scope_resolution_provenance(
    transaction: &mut PgConnection,
    owner_job_execution_id: JobExecutionId,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_scope_resolution_provenance \
         WHERE owner_job_execution_id = $1)",
    )
    .bind(job_database_id(owner_job_execution_id)?)
    .fetch_one(transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)
}

pub(crate) async fn copy_forward_job_scope_resolution_provenance(
    transaction: &mut PgConnection,
    source_job_execution_id: JobExecutionId,
    target_job_execution_id: JobExecutionId,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO oxide_batch.ob_scope_resolution_provenance \
         (owner_job_execution_id, owner_step_execution_id, scope_kind, component_id, input_name, \
          source_kind, source_parameter_name, source_node_id, source_framework_field, \
          source_job_execution_id, source_step_execution_id, source_execution_version, \
          source_schema, source_schema_version, source_path) \
         SELECT $2, NULL, provenance.scope_kind, provenance.component_id, provenance.input_name, \
          provenance.source_kind, provenance.source_parameter_name, provenance.source_node_id, \
          provenance.source_framework_field, \
          CASE WHEN provenance.source_kind = 'framework' THEN $2 \
               ELSE provenance.source_job_execution_id END, \
          provenance.source_step_execution_id, provenance.source_execution_version, \
          provenance.source_schema, provenance.source_schema_version, provenance.source_path \
         FROM oxide_batch.ob_scope_resolution_provenance provenance \
         WHERE provenance.owner_job_execution_id = $1 \
           AND provenance.owner_step_execution_id IS NULL \
           AND provenance.scope_kind = 'job'",
    )
    .bind(job_database_id(source_job_execution_id)?)
    .bind(job_database_id(target_job_execution_id)?)
    .execute(transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    Ok(())
}

pub(crate) async fn copy_forward_step_scope_resolution_provenance(
    transaction: &mut PgConnection,
    source_step_execution_id: StepExecutionId,
    target_job_execution_id: JobExecutionId,
    target_step_execution_id: StepExecutionId,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO oxide_batch.ob_scope_resolution_provenance \
         (owner_job_execution_id, owner_step_execution_id, scope_kind, component_id, input_name, \
          source_kind, source_parameter_name, source_node_id, source_framework_field, \
          source_job_execution_id, source_step_execution_id, source_execution_version, \
          source_schema, source_schema_version, source_path) \
         SELECT $2, $3, provenance.scope_kind, provenance.component_id, provenance.input_name, \
          provenance.source_kind, provenance.source_parameter_name, provenance.source_node_id, \
          provenance.source_framework_field, \
          CASE WHEN provenance.source_kind = 'framework' THEN $2 \
               ELSE provenance.source_job_execution_id END, \
          CASE WHEN provenance.source_kind = 'framework' THEN $3 \
               ELSE provenance.source_step_execution_id END, \
          provenance.source_execution_version, provenance.source_schema, \
          provenance.source_schema_version, provenance.source_path \
         FROM oxide_batch.ob_scope_resolution_provenance provenance \
         WHERE provenance.owner_step_execution_id = $1 \
           AND provenance.scope_kind = 'step'",
    )
    .bind(step_database_id(source_step_execution_id)?)
    .bind(job_database_id(target_job_execution_id)?)
    .bind(step_database_id(target_step_execution_id)?)
    .execute(transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    Ok(())
}

async fn validate_source_instance(
    transaction: &mut PgConnection,
    entry: &ScopeResolutionProvenance,
) -> Result<(), RepositoryError> {
    let valid = match entry.source() {
        ScopeResolutionSource::JobParameter {
            job_execution_id, ..
        }
        | ScopeResolutionSource::JobContext {
            job_execution_id, ..
        } => {
            if *job_execution_id == entry.owner_job_execution_id() {
                true
            } else {
                sqlx::query_scalar(
                    "SELECT EXISTS( \
                     SELECT 1 FROM oxide_batch.ob_job_execution owner \
                     JOIN oxide_batch.ob_job_execution source \
                       ON source.id = $2 AND source.job_instance_id = owner.job_instance_id \
                     WHERE owner.id = $1)",
                )
                .bind(job_database_id(entry.owner_job_execution_id())?)
                .bind(job_database_id(*job_execution_id)?)
                .fetch_one(&mut *transaction)
                .await
                .map_err(|_| RepositoryError::Unavailable)?
            }
        }
        ScopeResolutionSource::StepContext {
            step_execution_id: Some(step_id),
            ..
        } => sqlx::query_scalar(
            "SELECT EXISTS( \
                 SELECT 1 FROM oxide_batch.ob_job_execution owner \
                 JOIN oxide_batch.ob_step_execution source_step ON source_step.id = $2 \
                 JOIN oxide_batch.ob_job_execution source_job \
                   ON source_job.id = source_step.job_execution_id \
                  AND source_job.job_instance_id = owner.job_instance_id \
                 WHERE owner.id = $1)",
        )
        .bind(job_database_id(entry.owner_job_execution_id())?)
        .bind(step_database_id(*step_id)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| RepositoryError::Unavailable)?,
        ScopeResolutionSource::StepContext {
            step_execution_id: None,
            ..
        }
        | ScopeResolutionSource::Framework { .. } => true,
        _ => return Err(RepositoryError::ScopeResolutionStateCorrupt),
    };
    if !valid {
        return Err(RepositoryError::ScopeResolutionStateCorrupt);
    }
    Ok(())
}

async fn insert_provenance(
    transaction: &mut PgConnection,
    entry: &ScopeResolutionProvenance,
) -> Result<(), RepositoryError> {
    let fields = SourceFields::from_source(entry.source())?;
    sqlx::query(
        "INSERT INTO oxide_batch.ob_scope_resolution_provenance          (owner_job_execution_id, owner_step_execution_id, scope_kind, component_id, input_name,           source_kind, source_parameter_name, source_node_id, source_framework_field,           source_job_execution_id, source_step_execution_id, source_execution_version,           source_schema, source_schema_version, source_path)          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)          ON CONFLICT DO NOTHING",
    )
    .bind(job_database_id(entry.owner_job_execution_id())?)
    .bind(entry.owner_step_execution_id().map(step_database_id).transpose()?)
    .bind(entry.scope().as_str())
    .bind(entry.component().as_str())
    .bind(entry.input().as_str())
    .bind(fields.kind)
    .bind(fields.parameter)
    .bind(fields.node)
    .bind(fields.framework)
    .bind(fields.job_execution_id)
    .bind(fields.step_execution_id)
    .bind(fields.execution_version)
    .bind(fields.schema)
    .bind(fields.schema_version)
    .bind(fields.path)
    .execute(transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    Ok(())
}

struct SourceFields {
    kind: &'static str,
    parameter: Option<String>,
    node: Option<String>,
    framework: Option<&'static str>,
    job_execution_id: Option<i64>,
    step_execution_id: Option<i64>,
    execution_version: Option<i64>,
    schema: Option<String>,
    schema_version: Option<i32>,
    path: Option<Json<Value>>,
}

impl SourceFields {
    fn from_source(source: &ScopeResolutionSource) -> Result<Self, RepositoryError> {
        match source {
            ScopeResolutionSource::JobParameter {
                job_execution_id,
                parameter,
            } => Ok(Self {
                kind: "job_parameter",
                parameter: Some(parameter.as_str().to_owned()),
                node: None,
                framework: None,
                job_execution_id: Some(job_database_id(*job_execution_id)?),
                step_execution_id: None,
                execution_version: None,
                schema: None,
                schema_version: None,
                path: None,
            }),
            ScopeResolutionSource::JobContext {
                job_execution_id,
                execution_version,
                schema,
                schema_version,
                path,
            } => Ok(Self {
                kind: "job_context",
                parameter: None,
                node: None,
                framework: None,
                job_execution_id: Some(job_database_id(*job_execution_id)?),
                step_execution_id: None,
                execution_version: Some(version_database_value(*execution_version)?),
                schema: Some(schema.as_str().to_owned()),
                schema_version: Some(schema_version_database_value(*schema_version)?),
                path: Some(Json(json!(path))),
            }),
            ScopeResolutionSource::StepContext {
                node,
                step_execution_id,
                execution_version,
                schema,
                schema_version,
                path,
            } => Ok(Self {
                kind: "step_context",
                parameter: None,
                node: Some(node.as_str().to_owned()),
                framework: None,
                job_execution_id: None,
                step_execution_id: step_execution_id.map(step_database_id).transpose()?,
                execution_version: execution_version.map(version_database_value).transpose()?,
                schema: Some(schema.as_str().to_owned()),
                schema_version: Some(schema_version_database_value(*schema_version)?),
                path: Some(Json(json!(path))),
            }),
            ScopeResolutionSource::Framework {
                source,
                job_execution_id,
                step_execution_id,
            } => Ok(Self {
                kind: "framework",
                parameter: None,
                node: None,
                framework: Some(source.as_str()),
                job_execution_id: Some(job_database_id(*job_execution_id)?),
                step_execution_id: step_execution_id.map(step_database_id).transpose()?,
                execution_version: None,
                schema: None,
                schema_version: None,
                path: None,
            }),
            _ => Err(RepositoryError::ScopeResolutionStateCorrupt),
        }
    }
}

fn decode_provenance(
    row: &sqlx::postgres::PgRow,
) -> Result<ScopeResolutionProvenance, RepositoryError> {
    let scope = match text(row, "scope_kind")?.as_str() {
        "job" => ScopeKind::Job,
        "step" => ScopeKind::Step,
        _ => return Err(RepositoryError::ScopeResolutionStateCorrupt),
    };
    let component = ScopedComponentId::new(text(row, "component_id")?)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?;
    let input = ParameterName::new(text(row, "input_name")?)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?;
    let owner_job_execution_id = job_id(row, "owner_job_execution_id")?;
    let owner_step_execution_id = optional_step_id(row, "owner_step_execution_id")?;
    let source = decode_source(row)?;
    ScopeResolutionProvenance::new(
        scope,
        component,
        input,
        owner_job_execution_id,
        owner_step_execution_id,
        source,
    )
    .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
}

fn decode_source(row: &sqlx::postgres::PgRow) -> Result<ScopeResolutionSource, RepositoryError> {
    match text(row, "source_kind")?.as_str() {
        "job_parameter" => Ok(ScopeResolutionSource::JobParameter {
            job_execution_id: job_id(row, "source_job_execution_id")?,
            parameter: ParameterName::new(required_optional_text(row, "source_parameter_name")?)
                .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?,
        }),
        "job_context" => ScopeResolutionSource::job_context(
            job_id(row, "source_job_execution_id")?,
            execution_version(row, "source_execution_version")?,
            StateSchemaId::new(required_optional_text(row, "source_schema")?)
                .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?,
            schema_version(row, "source_schema_version")?,
            path(row)?,
        )
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt),
        "step_context" => ScopeResolutionSource::step_context(
            NodeId::new(required_optional_text(row, "source_node_id")?)
                .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?,
            optional_step_id(row, "source_step_execution_id")?,
            optional_execution_version(row, "source_execution_version")?,
            StateSchemaId::new(required_optional_text(row, "source_schema")?)
                .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?,
            schema_version(row, "source_schema_version")?,
            path(row)?,
        )
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt),
        "framework" => Ok(ScopeResolutionSource::Framework {
            source: framework_source(&required_optional_text(row, "source_framework_field")?)?,
            job_execution_id: job_id(row, "source_job_execution_id")?,
            step_execution_id: optional_step_id(row, "source_step_execution_id")?,
        }),
        _ => Err(RepositoryError::ScopeResolutionStateCorrupt),
    }
}

fn framework_source(value: &str) -> Result<ScopeFrameworkSource, RepositoryError> {
    match value {
        "job_instance_id" => Ok(ScopeFrameworkSource::JobInstanceId),
        "job_execution_id" => Ok(ScopeFrameworkSource::JobExecutionId),
        "step_execution_id" => Ok(ScopeFrameworkSource::StepExecutionId),
        "attempt" => Ok(ScopeFrameworkSource::Attempt),
        "definition_revision" => Ok(ScopeFrameworkSource::DefinitionRevision),
        "definition_fingerprint" => Ok(ScopeFrameworkSource::DefinitionFingerprint),
        "logical_node_id" => Ok(ScopeFrameworkSource::LogicalNodeId),
        "step_name" => Ok(ScopeFrameworkSource::StepName),
        _ => Err(RepositoryError::ScopeResolutionStateCorrupt),
    }
}

fn path(row: &sqlx::postgres::PgRow) -> Result<Vec<String>, RepositoryError> {
    let value = row
        .try_get::<Option<Json<Value>>, _>("source_path")
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?
        .ok_or(RepositoryError::ScopeResolutionStateCorrupt)?
        .0;
    let array = value
        .as_array()
        .ok_or(RepositoryError::ScopeResolutionStateCorrupt)?;
    let mut segments = Vec::with_capacity(array.len());
    for value in array {
        segments.push(
            value
                .as_str()
                .ok_or(RepositoryError::ScopeResolutionStateCorrupt)?
                .to_owned(),
        );
    }
    if !oxide_batch_core::selector_path_is_valid(&segments) {
        return Err(RepositoryError::ScopeResolutionStateCorrupt);
    }
    Ok(segments)
}

fn text(row: &sqlx::postgres::PgRow, column: &str) -> Result<String, RepositoryError> {
    row.try_get::<String, _>(column)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
}

fn required_optional_text(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<String, RepositoryError> {
    row.try_get::<Option<String>, _>(column)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?
        .ok_or(RepositoryError::ScopeResolutionStateCorrupt)
}

fn job_id(row: &sqlx::postgres::PgRow, column: &str) -> Result<JobExecutionId, RepositoryError> {
    let raw = row
        .try_get::<Option<i64>, _>(column)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?
        .ok_or(RepositoryError::ScopeResolutionStateCorrupt)?;
    let value = u64::try_from(raw).map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?;
    JobExecutionId::new(value).map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
}

fn optional_step_id(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<Option<StepExecutionId>, RepositoryError> {
    row.try_get::<Option<i64>, _>(column)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?
        .map(|raw| {
            u64::try_from(raw)
                .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
                .and_then(|value| {
                    StepExecutionId::new(value)
                        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
                })
        })
        .transpose()
}

fn optional_execution_version(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<Option<ExecutionVersion>, RepositoryError> {
    row.try_get::<Option<i64>, _>(column)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?
        .map(|raw| {
            u64::try_from(raw)
                .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
                .map(ExecutionVersion::new)
        })
        .transpose()
}

fn execution_version(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<ExecutionVersion, RepositoryError> {
    let raw = row
        .try_get::<Option<i64>, _>(column)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?
        .ok_or(RepositoryError::ScopeResolutionStateCorrupt)?;
    let value = u64::try_from(raw).map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?;
    Ok(ExecutionVersion::new(value))
}

fn schema_version(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<StateSchemaVersion, RepositoryError> {
    let raw = row
        .try_get::<Option<i32>, _>(column)
        .map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?
        .ok_or(RepositoryError::ScopeResolutionStateCorrupt)?;
    let value = u32::try_from(raw).map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)?;
    StateSchemaVersion::new(value).map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
}

fn job_database_id(id: JobExecutionId) -> Result<i64, RepositoryError> {
    i64::try_from(id.get()).map_err(|_| RepositoryError::IdentifierOutOfRange {
        kind: IdentifierKind::JobExecution,
        value: id.get(),
    })
}

fn step_database_id(id: StepExecutionId) -> Result<i64, RepositoryError> {
    i64::try_from(id.get()).map_err(|_| RepositoryError::IdentifierOutOfRange {
        kind: IdentifierKind::StepExecution,
        value: id.get(),
    })
}

fn version_database_value(version: ExecutionVersion) -> Result<i64, RepositoryError> {
    i64::try_from(version.get()).map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
}

fn schema_version_database_value(version: StateSchemaVersion) -> Result<i32, RepositoryError> {
    i32::try_from(version.get()).map_err(|_| RepositoryError::ScopeResolutionStateCorrupt)
}
