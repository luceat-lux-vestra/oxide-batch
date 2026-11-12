use futures_executor::block_on;
use oxide_batch::{
    ExplorerError, ExplorerRepository, JobExecutionId, JobExplorer, PageRequest, PageSize,
};

#[derive(Clone, Copy, Debug)]
struct MinimalExplorer;

impl ExplorerRepository for MinimalExplorer {}

#[test]
fn omitted_paginated_query_fails_closed() {
    let explorer = JobExplorer::new(MinimalExplorer);
    let request = PageRequest::first(PageSize::new(1).expect("one row is within the page bound"));

    let result = block_on(explorer.list_job_names(&request));

    assert_eq!(result.unwrap_err(), ExplorerError::UnsupportedCapability);
}

#[test]
fn omitted_point_query_fails_closed() {
    let explorer = JobExplorer::new(MinimalExplorer);
    let execution_id = JobExecutionId::new(1).expect("nonzero execution id");

    let result = block_on(explorer.get_execution(execution_id));

    assert_eq!(result.unwrap_err(), ExplorerError::UnsupportedCapability);
}
