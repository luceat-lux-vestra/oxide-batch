from pathlib import Path
import re

p = Path('crates/oxide-batch/src/repository/postgres.rs')
text = p.read_text()

anchor = 'fn nested_job_link_select(suffix: &str) -> String {\n'
if text.count(anchor) != 1:
    raise SystemExit(f'helper anchor mismatch: {text.count(anchor)}')
helper = r'''async fn select_nested_child_for_request(
    unit: &mut PostgresUnitOfWork<'_>,
    request: &NestedJobLinkRequest,
) -> Result<JobExecution, RepositoryError> {
    let child_name = request
        .child_definition()
        .job_name()
        .cloned()
        .ok_or(RepositoryError::NestedJobStateCorrupt)?;
    let child_key = JobInstanceKey::new(child_name, request.child_parameters());
    let child_instance = unit
        .select_or_create_job_instance(&child_key)
        .await?
        .instance()
        .clone();
    let latest = unit.job_executions(child_instance.id()).await?.pop();
    let child_execution = match latest {
        None => {
            unit.create_job_execution_with_definition(
                child_instance.id(),
                request.child_definition(),
            )
            .await?
        }
        Some(latest) => {
            let definition =
                load_execution_definition(&mut **unit.transaction()?, latest.id()).await?;
            if &definition != request.child_definition() {
                return Err(RepositoryError::NestedJobStateCorrupt);
            }
            match latest.metadata().status() {
                BatchStatus::Completed => {
                    let parameters =
                        load_execution_parameters(&mut **unit.transaction()?, latest.id()).await?;
                    if &parameters != request.child_parameters() {
                        return Err(RepositoryError::NestedJobStateCorrupt);
                    }
                    latest
                }
                BatchStatus::Failed | BatchStatus::Stopped => {
                    unit.create_job_execution_with_definition(
                        child_instance.id(),
                        request.child_definition(),
                    )
                    .await?
                }
                BatchStatus::Starting
                | BatchStatus::Started
                | BatchStatus::Stopping
                | BatchStatus::Unknown => {
                    return Err(RepositoryError::NestedJobChildUnresolved {
                        child_execution_id: latest.id(),
                        status: latest.metadata().status(),
                    });
                }
                _ => return Err(RepositoryError::NestedJobStateCorrupt),
            }
        }
    };
    store_execution_parameters(
        &mut **unit.transaction()?,
        child_execution.id(),
        request.child_parameters(),
    )
    .await?;
    Ok(child_execution)
}

'''
text = text.replace(anchor, helper + anchor, 1)

pattern = re.compile(
    r"    fn create_nested_job_link<'a>\(.*?\n    fn continue_nested_job_link<'a>\(",
    re.S,
)
replacement = r'''    fn create_nested_job_link<'a>(
        &'a mut self,
        request: &'a NestedJobLinkRequest,
    ) -> BoxFuture<'a, Result<NestedJobLink, RepositoryError>> {
        Box::pin(async move {
            let parent_execution_id = database_id(
                request.parent_job_execution_id().get(),
                IdentifierKind::JobExecution,
            )?;
            let parent_instance: i64 = sqlx::query_scalar(
                "SELECT job_instance_id FROM oxide_batch.ob_job_execution \
                 WHERE id = $1 FOR UPDATE",
            )
            .bind(parent_execution_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobExecutionNotFound {
                id: request.parent_job_execution_id(),
            })?;
            if u64::try_from(parent_instance).map_err(|_| RepositoryError::NestedJobStateCorrupt)?
                != request.parent_job_instance_id().get()
            {
                return Err(RepositoryError::NestedJobStateCorrupt);
            }
            if let Some(existing) = load_nested_job_link(
                &mut **self.transaction()?,
                request.parent_job_execution_id(),
                request.node_id(),
                true,
            )
            .await?
            {
                let parameters = load_execution_parameters(
                    &mut **self.transaction()?,
                    existing.child_job_execution_id(),
                )
                .await?;
                if existing.child_definition() != request.child_definition()
                    || &parameters != request.child_parameters()
                {
                    return Err(RepositoryError::NestedJobStateCorrupt);
                }
                return Ok(existing);
            }

            let child_execution = select_nested_child_for_request(self, request).await?;
            insert_nested_job_link(
                &mut **self.transaction()?,
                request.parent_job_instance_id(),
                request.parent_job_execution_id(),
                request.node_id(),
                child_execution.id(),
                request.linked_at(),
                None,
            )
            .await?;
            load_nested_job_link(
                &mut **self.transaction()?,
                request.parent_job_execution_id(),
                request.node_id(),
                false,
            )
            .await?
            .ok_or(RepositoryError::NestedJobStateCorrupt)
        })
    }

    fn continue_nested_job_link<'a>('''
text, count = pattern.subn(replacement, text, count=1)
if count != 1:
    raise SystemExit(f'create_nested_job_link replacement count {count}')
p.write_text(text)
