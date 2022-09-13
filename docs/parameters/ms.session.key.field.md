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

This property takes the form a JsonObject with a **name**, a **condition**, a **failCondition**,
and a **initValue**.

it takes the form a Json object with a "name", "condition", and "failCondition".
- **name** specifies the field in response that gives session info, it is required
- **condition** specifies when the session should stop.
- **failCondition** specifies when the session should fail.
- **initValue** specifies the initial value of the session key. It is a string value such as "MDAzMzIwMDAwMXlqN3ZEQUFR"
- "condition", "failCondition", and "initValue" are optional
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

The **initValue** parameter contains an optional initial session key value. 
When **initValue** is not provided and a `session` type variable is defined in
[ms.parameters](ms.parameters.md), the default behavior is used in which on the 
initial request the session key is not included in the request URI or body, and on 
subsequent requests it is included. When an initial value is provided, this value
will be passed to the `session` type variable on the initial run. This can be useful
in certain scenarios:
- To start cursor pagination from a specific page key when working with one work unit
- to use cursor pagination on a POST request URI in which typically the variables defined in
  [ms.parameters](ms.parameters.md) are passed to the request body. 

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

In the following, the session key is used as a cursor in the URI for a POST request. We can reference the session variable
defined in [ms.parameters](ms.parameters.md) inside [ms.source.uri](ms.source.uri.md) such as `<URI>/?next_page={{next_page}}`.
On the initial request, the cursor value must be present otherwise the next_page variable would not be substituted. So, an
initial value is defined in **ms.session.key.field** using "initValue". In this example, the API accepts an empty value
for cursor as the same thing as no value. "" is substituted in as the initial cursor. Subsequent requests for cursor pagination
work as normal.
- `ms.source.uri`=`https://<URI>/?next_page={{next_page}}'`
- `ms.session.key.field={"name": "next_page", "condition": {"regexp": ""}, "initValue": ""}`
- `ms.parameters=[{"name":"next_page", "type":"session"}]`

[back to summary](summary.md#mssessionkeyfield)