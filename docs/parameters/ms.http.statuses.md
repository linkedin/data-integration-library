# ms.http.statuses

**Tags**: 
[http](categories.md#http-properties)

**Type**: string

**Format**: A JsonObject

**Default value**: `{"success":[200,201,202], "pagination_error":[401]}"`

**Related**:

## Description

`ms.http.statuses` defines status codes that should be treated as success, 
warning, or error. 

It is a JsonObject with the following attributes:

- **success**: a list of status codes that should be deemed as successful 
- **warning**: a list of status codes that should be deemed as successful with warning
- **error**: a list of status codes that should be deemed as errors
- **pagination_error**: a list of status codes that should be deemed as transient errors,
and requires the DIL to refresh authentication token in next page 

By default, if this parameter is not set, 200 (OK), 201 (CREATED), and 202 (ACCEPTED)
will be treated as success; anything else below 400 will be treated as warning; and
anything 400 and above will be treated as error. Warnings will be logged but will not
cause job failure. Errors will cause job failure.

In cases where 4xx codes, like 404 (NOT FOUND), happened frequently, and a failure is
not desirable, exceptions can be reclassified as `warning`.
For example, if 404 is to be treated as `warning` instead of an `error`, it
can be configured as 
- `ms.http.statuses={"success": [200, 201, 202], "warning": [404]}`

### Example

In following configuration, we make 404 an warning, and make 206 a failure indicating
that partial content is not acceptable:
- `ms.http.statuses={"success": [200], "warning": [404], "error": [206]}`

[back to summary](summary.md#mshttpstatuses)
