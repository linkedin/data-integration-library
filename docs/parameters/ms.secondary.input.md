# ms.secondary.input

**Tags**: 
[watermark & work unit](categories.md#watermark-work-unit-properties),
[authentication](authentication-properties.md)

**Type**: string

**Format**: A JsonArray of JsonObjects, with each JsonObject defines
one secondary input.

**Default value**: "[]" (a blank JsonArray)

**Related**:

## Description

Secondary inputs provides additional directives to job execution, in addition to
the primary inputs of job execution, which is its metadata, i.e, job configurations. 

`ms.secondary.input` property has attributes to support the following functions:

- **location**: from where the HDFS data will be loaded as secondary input
- **fields**: fields that needs to be extracted and added into the work unit state
- **filters**: rules to include/exclude records from secondary input
- **Tags**: specify the type of secondary input

The following are filtering rules:

 - if multiple fields are filtered, the relationship is AND, that means 
 all condition must be met
 - if a filter is defined on a field, and field value is NULL, the record is rejected
 - if a filter is defined on a field, and the field value is not NULL, the 
 record will be rejected if its value doesn't match the pattern
 - if no filter is defined on a field, the default filter ".*" is applied to 
 the field, and NULL values are accepted

DIL designed 3 categories of secondary inputs:

- **activation**: `activation` secondary input creates work units
- **authentication**: `authentication` secondary input provides authentication information,
like access tokens
- **payload**: `payload` secondary input specifies the location to pick additional data that
will only be interpreted by connection. 

### Activation Secondary Input

`activation` secondary inputs are used to "activate" the job. That means the job will
generate work units based on the given values. 

For example, if we have a file of a list of Ids to extract from a source, then
we can define an `activation` category of secondary input based on the file.

- `ms.secondary.input=[{"path":"/path","fields":["id"],"category":"activation"}]`

At the beginning of job execution, DIL will read the list file and retrieve the `id`s.
Then each `id` leads to the generation of one work unit, which has an attribute `id:value`.

### Authentication Secondary Input

`authentication` secondary inputs are used to read dynamic authentication credentials or tokens 
from a storage location, so that they don't need to be coded in metadata, which is static. 

Typical usage is that we use a separate job to get a refreshed access token, and save 
the access token on media, then the primary integration job can read the access token
for subsequent requests. 

`authentication` secondary input doesn't generate work units, and it is passed to all
work units, i.e, all work units get the same authentication credentials/tokens. 

### Payload Secondary Input

`payload` secondary inputs are used to specify raw payload locations.

Payloads are simply passed to connections without processing. The connection will decide 
what to do about it. 
For example, HTTP connection will read the records from the payload, and attach 1
row to 1 HTTP request. If there are multiple rows, HTTP connection will page (see [pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md))
through them. Therefore, each row of the payload is processed by 1 HTTP request. 

`payload` secondary input is typically used in the egression flows. If there are many
rows to send out, they can be "batched" so that the payload file has fewer number of
records. 
 
### Examples

In the following, we have a file with a list of ids and their statuses. We
expect the job will generate 1 work unit for each id of status "OK" or "Success".
The gobblin job will read records from that location and extract the two 
fields and inject them into the work units. Each work unit then has 2 variables,
"id" and "tempId", that can be used as request parameters.

- ` ms.secondary.input=[{
 "path": "/path/to/hdfs/inputFileDir",
 "fields": ["id", "tempId"],
 "filters": {"status": "(OK|Success)"},
 "category" "activation"
 }]
`

In the following, we only process certain ids from a list. This is typically used
in back fill.   

- `ms.secondary.input=[{"path": "/path/ids", "fields": ["id"], "filters": {"id": "(19|28|89)"}}]`

In the following, we egress the normalized records from a prior job (`${preprocess.table.name}`)

- `ms.secondary.input=[{"path": "${job.dir}/${extract.namespace}/${preprocess.table.name}", "fields": ["conversions"], "category": "payload"}]`

[back to summary](summary.md#mssecondaryinput)