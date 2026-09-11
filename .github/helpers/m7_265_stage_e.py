from pathlib import Path

path = Path('crates/oxide-batch/tests/nested_job_runtime.rs')
s = path.read_text()
s = s.replace(
    '    JobParameter, JobParameters, JobRepository, MissingParameterPolicy, NestedJobMappingFailure,\n',
    '    JobName, JobParameter, JobParameters, JobRepository, MissingParameterPolicy, NestedJobMappingFailure,\n',
)
s = s.replace(
    '    ParameterName, ParameterRole, ParameterValue, ParameterValueKind, RepositoryUnitOfWork,\n',
    '    ParameterName, ParameterRole, ParameterValue, ParameterValueKind,\n',
)
path.write_text(s)
