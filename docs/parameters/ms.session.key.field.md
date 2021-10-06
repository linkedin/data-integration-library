# ms.session.key.field

**Tags**:
[source](categories.md#source-properties),
[pagination](categories.md#pagination-properties)

**Type**: string

**Format**: A JsonObject

**Default value**: "{}" (a blank JsonObject)

**Related**:
- [key concept: pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md)
- [ms.pagination](ms.pagination.md)
- [ms.total.count.field](ms.total.count.field.md)

## Description

Session is a state management mechanism over stateless connections.
For example, although Restful API is stateless, data sources can maintain 
a session in backend by a status field, a session cursor, or through 
[pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md).

`ms.session.key.field` specifies the key field in response in order to retrieve the 
status for session control and the condition for termination.

This property takes the form a JsonObject with a **name**, a **condition**, 
and a **failCondition**.

it takes the form a Json object with a "name", "condition", and "failCondition".
- **name** specifies the field in response that gives session info, it is required
- **condition** specifies when the session should stop.
- **failCondition** specifies when the session should fail.
- "condition" and "failCondition" are optional
- "condition" takes precedence over "failCondition"

The "name" element of ms.session.key.field is one of following:

- For API returning Json response, "name" should be the Json element that 
decides next action or next fetching start position.
- For API returning CSV response, "name" should be the HTTP header tag 
that decides next action or next fetching start position

A condition can be a regular expression or a formula (TODO). Currently, 
only regular expression is supported.

When only session control is enabled but no pagination, the extractor 
will keep consuming data from the source until:

- the stop condition is met, or
- the session timed out

When both session and pagination are enabled, the extractor will 
keep consuming data from the source until

- the stop condition is met, or
- the session timed out, or
- the total number of records are fetched, or
- a blank response is received from source, or
- an error/warning is received that indicates the response 
should not be treated as valid data

**Alert**: In that regard, when the source gives conflicting signal in turns of 
total expected rows and status, the data can have duplicate, and actual 
extracted rows in log file should show more rows extracted than expected.

A session can timeout before the stop condition is met. The timeout 
is controlled by property [ms.wait.timeout.seconds](ms.wait.timeout.seconds.md),
which has a default value of 600 seconds. 

This is useful for [asynchronous ingestion](https://github.com/linkedin/data-integration-library/blob/master/docs/patterns/asynchronous-ingestion-pattern.md), 
for example, we can have the stop condition set 
as the status value of a successful request, such as "completed" or "ready". 
When the stop condition is not met during the timeout period, the session 
will time out (error), and the session will abort (stop and fail).

Session key value can come from the Json payload for Rest API, or from an 
HTTP header for SOAP API. Rest API returns JSON response. SOAP API 
returns XML or CSV response. When SOAP API is instructed to return CSV 
response, the session key value mostly is included in an HTTP header.

Session key can be used in the following 2 scenarios:

- as a session control mechanism over a series of independent requests 
until the stop condition is met (above)
- as a session control mechanism over a sequence of dependent requests, 
with the session key value of first request fed into the next request as 
input. That means the fetched value of session key, from the 
`ms.session.key.field`, can be fed into variables of type [session](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/session-control.md). 

Variables are defined in [ms.parameters](ms.parameters.md). 
When a `session` type variable is defined, it will implicitly add to 
URL parameters or request bodies if HttpSource is used. 
However, for the first HTTP request, because the 
session variable has a NULL value, it will not be used in the request.

### Examples

In the following, the session key provides a stop condition:
 
- `ms.session.key.field={"name": "hasMore", "condition": {"regexp": "false|False"}}`

In the following, the session key is used as a cursor. A `session` type variable
`cursor` is defined. This variable will get the session key value 
in each page after the first page. When the `cursor` variable
is used in requests, it establishes a "session" on the source.
 
- `ms.session.key.field={"name": "records.cursor"}`
- `ms.parameters=[{"name":"fromDateTime","type":"watermark","watermark":"system","value":"low","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"},{"name":"toDateTime","type":"watermark","watermark":"system","value":"high","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"},{"name":"cursor","type":"session"}]`

In the following, say the source doesn't supply a total records or total pages 
to indicate when to finish ingesting data from it. Instead, in the 
source's response, it contains a boolean field called "end_of_stream". 
This field indicates whether the data source is exhausted or not.
Only when this field has the value "true", the source has finished 
streaming data. 

- `ms.session.key.field={"name": "end_of_stream", "condition": {"regexp": "^true$"}}`

[back to summary](summary.md#mssessionkeyfield)