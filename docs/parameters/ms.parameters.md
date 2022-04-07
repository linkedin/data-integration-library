# ms.parameters

**Tags**: 
[source](categories.md#source-properties)
[authentication](categories.md#authentication-properties)

**Type**: string

**Format**: A JsonArray of JsonObjects

**Default value**: "[]" (a blank JsonArray)

## Related 
- [key concept: variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variables.md)
- [key concept: watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/watermark.md)
- [key concept: pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md)
- [job property: ms.session.key.field](ms.session.key.field.md)
- [job property: ms.watermark](ms.watermark.md)
- [job property: ms.pagination](ms.pagination.md)

## Description 

ms.parameter defines a list of **named parameters**,
which are also [**variables**](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variables.md) 
that can be referenced in other configuration properties 
using the syntax of double brackets **{{variableName}}**.

Parameters are special variables that are used to execute requests (Http request, JDBC request, and Sftp request, etc).
Other variables are just kept internally, they are not used to execute requests.

Parameters can have either static values or dynamically derived 
values using a formula. 

Parameters can recursively use other defined variables in definition, 
but the recursion level shall not be more than 1.   

Parameters will have values in string format. Presently other formats, 
like integer, are not considered, instead, integer values
will be carried as strings. 

### Types of Parameters

The following types of parameters are designed: 

- **primitive**: `primitive` type is for those cases where the value should not be quoted in the final request. 
While `list` type is for those cases where the value should be quoted. 
for e.g. for parameters like {"id":114027}, {"id":true}, {"id":93.3453}

- **list**: list is the default type, which means a primitive string.

- **object**: a `object` parameter contains recursively defined parameters.

- **watermark**, a `watermark` parameter derives its value from [watermarks](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/watermark.md). 
Because watermarks are work unit specific, so parameters defined as `watermark` type 
would have different values for each work unit. When define a parameter off a 
time watermark, the value can be "low" or "high", which means using 
low watermark or high watermark values, and the format can be "**datetime**", 
"**epoc-second**", or **unspecified**. If format is not specified, it will be 
**epoch millisecond**. That's the default format. When the format is "**datetime**", 
a pattern can specified for the output. The pattern is normally ISO 
Java date time string. Timezone is supported. Milli-seconds are supported. 
Micro-seconds are not supported, but you can format the milli-second 
in 6 digits, i.e., the last 3 digits will be just 0s.
 
- **session**, a `session` parameter derives its value from the 
[session control](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/session-control.md) variable.

- **pagestart**, a `pagestart` parameter derives its value from the starting row 
number of next page during the pagination process. 
The page-start parameter can be named differently in each application. 
Examples are "offset", "start", "from" etc.
 
- **pagesize**, a `pagesize` parameter derives its value from the given 
pagination size during the pagination process. 
The page-size parameter can be named differently in each application. 
Examples are "limit", "size" etc.

- **pageno**, a `pageno` parameter derives its value from the page number of 
next page during the pagination process.
The page-no variable can be named differently in each application. Examples are "next", "page_number" etc.

- **jsonarray**, a `jsonarray` parameter defines are raw JsonArray. A JsonArray
parameter can take a very complex JsonArray object, and it doesn't require
each element be recursively defined, like what `object` type variables do. 
For example: `ms.parameters=[{'name':'groupBys','type':'jsonarray','value':[{'heading':'CASE_ID','dimensionName':'CASE_ID','groupType':'FIELD','details':{}}, {'heading':'CASE','dimensionName':'CASE','groupType':'FIELD','details':{}}]`

- **jsonobject**, a `jsonobject` parameter defines are raw JsonObject. 
A JsonObject parameter can take a very complex JsonObject object, 
and it doesn't require  each element be recursively defined, 
like what `object` type parameter does. For example:  
`ms.parameters=[{"name":"token","type":"JSONOBJECT", "value": {"client_id": "999999", "scopes": ["read", "write"]}}]`  
**Note** the value of "value" element is not quoted, otherwise, it will be 
treated as a primitive string.
  
### Sending Dynamic Requests
  
**Terminology**: in following description, we call parameters used in URI as URI Parameters.

_The following is explained using HTTP requests as example, the variable substitution mechanism works 
for JDBC and S3._

For HTTP requests, variables will be used to form the final URI. In such case, the variable can be 
used in the URI path segments or as URI parameter.

- When used in URI path, the variable name need to be specified in [URI template](ms.source.uri.md) 
as a variable contained in {{}}.

- When used as URI parameters, the variable name can be specified in URI template. If not used in the URI template, 
the variable name and its derived value will be coded as KV pairs, then these KV pair will be sent to the 
data source as request parameters; therefore, the variable name need to be acceptable to source. Keep in mind, some data sources will
reject unknown request parameters. This is particularly important for HTTP POST and HTTP PUT requests, 
the variable name will be used as-is in the form of "variable name": "variable value" in the request body; 
therefore, the variable name have to match URI source expectation.
 
For example, if a source accepts URI like http://domainname/endpoint?cursor=xxxx, and the "cursor" parameter is optional, 
then the variable should be named as "cursor", and the URI template should be set as http://domainname/endpoint in pull file.
In such case, a "?cursor=xxx" will be appended to the final URI when cursor is present, otherwise, no URI parameter is added
to the URI. This is important because some data sources reject any session control request parameters in the first request. 

However, if the cursor URI parameter is not optional, the URI template could be coded as http://domain/endpoint?cursor={{p1}}, 
then the variable can be named as "p1", and the variable value will replace {{p1}} before the request is sent to the URI source.

## Examples 1: setting pagination in URI

In this example, the URI needs 3 mandatory variables, and they can be named as p1, p2, and p3.
We can configure the parameters as following:

`ms.source.uri=https://domain.com/api/bulk/2.0/syncs/{{p1}}/data?offset={{p2}}&limit={{p3}}` </p>

`ms.parameter=[
  {"name": "p1", "value": "3837498"},
  {"name": "p2", "type": "pagestart"},
  {"name": "p3", "type": "pagesize"}]` </p>

## Example 2: setting session control

In this example, the URI needs 1 optional variable, and the parameter has to be named as
required by the source. Here is the configuration:

`ms.source.uri=https://domain.com/users` </p>

`ms.parameter=[{"name":"cursor","type":"session"}]` 

The first request will be sent to URL `https://domain.com/users`. Assume the response from the 
first request is `{"cursor" : "582939"}`, then the second request will be sent 
to URL `https://domain.com/users?cursor=582939`


## Example 3: setting date range in request 

In this example, we want a date range to be passed to the request as query parameters, and we want 
each work unit to work on a different portion of the data range.

First, we partition the time watermark to daily work units. 

`ms.work.unit.partition=daily`

Then, we define variables in ms.parameter like this:

`ms.parameter=
[{"name":"fromDate","type":"watermark","watermark":"system","value":"low","format":"datetime","pattern":"yyyy-MM-dd"},
{"name":"toDate","type":"watermark","watermark":"system","value":"high","format":"datetime","pattern":"yyyy-MM-dd"}]
`
That will make sure each work unit has a "fromDate" and a "toDate" variable, and their values are assigned
based on the date range that the work unit processes. 

Then we define source endpoint looks like this:

`ms.source.uri=
https://api.zoom.us/v2/metrics/webinars?type=past&from={{fromDate}}&to={{toDate}}
`

In execution time, each day range will be processed by a work unit.

[back to summary](summary.md#msparameters)
