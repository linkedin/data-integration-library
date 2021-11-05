# Session Control

Session control is the mechanism to maintain state over a stateless connection. For example, Http connections
are stateless. So data ingestion from API often relies on session control. Session control in DIL has broad
usage. Typical scenarios of using session control are:

- [Asynchronous ingestion](../patterns/asynchronous-ingestion-pattern.md)
- [Pagination](../concepts/pagination.md)
- [Two-step file download ](../how-to/status-check-job.md)

Session control is configured through [ms.session.key.field](../parameters/ms.session.key.field.md). 
For example, the following specifies that the "result.status" field within the server response should
contain a session control token, and when the token is "complete" or "failed", the session should end. 

- `ms.session.key.field={"name": "result.status", "condition": {"regexp": "^complete$"}, "failCondition": {"regexp": "^failed$"}}`

## Asynchronous Ingestion

In the asynchronous ingestion scenario, state has to be maintained throughout the process even when the client is disconnected 
from the server. The server should keep a request ID, or session ID, or a cursor as session key, so that the client can refer to the 
session key in requests belonging to the same session. Please see [Asynchronous ingestion](../patterns/asynchronous-ingestion-pattern.md)
for more details.

## Pagination

Pagination is typically used to fetch a large dataset from cloud over HTTP, where
one fetch can only optimally get a limited chunk of data. In such case,
the data is fetched through a series of pages. For this to work, the server has to keep a state
about where the next page should start. The state maybe static or change after each page.
Please see [Pagination](../concepts/pagination.md) for more details. 

## Two-step File Download

In a 2-step file download scenarios, session control is used to ensure the expected file is ready before 
the download job actually kicks off. In this case, a session ends when the file is ready. So the session is also
status checking session. See [Two-step file download ](../how-to/status-check-job.md) for more details. 

[Back to Summary](summary.md#session-control)