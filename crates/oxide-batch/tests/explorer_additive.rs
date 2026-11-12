//! Contract tests for additive explorer-port defaults.

use std::error::Error;

use futures_executor::block_on;
use oxide_batch::{
    ExplorerError, ExplorerRepository, JobExecutionId, JobExplorer, PageRequest, PageSize,
};

#[derive(Clone, Copy, Debug)]
struct MinimalExplorer;

impl ExplorerRepository for MinimalExplorer {}

#[test]
fn omitted_paginated_query_fails_closed() -> Result<(), Box<dyn Error>> {
    let explorer = JobExplorer::new(MinimalExplorer);
    let request = PageRequest::first(PageSize::new(1)?);

    let result = block_on(explorer.list_job_names(&request));

    assert!(matches!(
        result,
        Err(ExplorerError::UnsupportedCapability)
    ));
    Ok(())
}

#[test]
fn omitted_point_query_fails_closed() -> Result<(), Box<dyn Error>> {
    let explorer = JobExplorer::new(MinimalExplorer);
    let execution_id = JobExecutionId::new(1)?;

    let result = block_on(explorer.get_execution(execution_id));

    assert!(matches!(
        result,
        Err(ExplorerError::UnsupportedCapability)
    ));
    Ok(())
}
