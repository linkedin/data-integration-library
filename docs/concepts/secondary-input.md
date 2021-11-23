# Secondary Input

Secondary inputs provides additional directives to job execution, in addition to
the primary inputs of job execution, which is its metadata, i.e, job configurations.

Secondary inputs are defined in [ms.secondary.input](../parameters/ms.secondary.input.md) property. 

Typically, secondary inputs are used to pass data between jobs, so that a job's output can be used
as input in the next job. 

Secondary inputs are widely used in the following scenarios:

- Use the output of an [Authentication Job](../how-to/authentication-job.md) in subsequent data extraction jobs in an OAuth2 authentication scenario
- Use the output of the first stage in the second stage of an [2-step file download flow](../patterns/two-step-file-download-pattern.md)
- Use the output of the request creation stage in subsequent status checking and data extraction stages in an [Async Ingestion flow](../patterns/asynchronous-ingestion-pattern.md).
- Specify the location of payloads in an egression flow

[Back to Summary](summary.md#secondary-input)