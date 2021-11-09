# How to Config Source and Authentication

In a DIL pipeline, source is the generally used to refer to the 3rd party system that we are interacting with.
This is easy to understand for ingestion, because in a data-ingest scenario, we are ingesting from the data "source".
For egression, it gets a little unclear. In a data-egress scenario, we send data out to the 3rd party system. So the
3rd party system should be "target". But what we emphasize here is that whenever we send something out, we should
expect to get something back at the same time, like a response with row statuses or success/failure code.

Information about the source, such as the protocol and host name, are specified via [ms.source.uri](../parameters/ms.source.uri.md).
The source URI can contain variables. Using variables makes it dynamic. For example, if a job needs to pull many
entities from the same API endpoint, the entity ID be represented with a variable so that we just need one job configuration,
not many repeating job configurations.

In the following example, the job can pull survey response data from a list of surveys, each represented by a "surveyId".
This example, also shows other variables, such as startDate and endDate, can be coded in the source URI.
See [Variable](../concepts/variables.md) for more details about variables.

- `ms.source.uri=https://decipherinc.com/api/v1/surveys/{{surveyId}}/data?format=csv&start={{startDate}}&end={{endDate}}`

Generally, when the source is dynamic(as above), it is recommended to start with a static value. After testing with the
static URI, variables can be devised to make the URI dynamic.

Please follow [Authentication Methods](../concepts/authentication-method.md) for authentication configuration details.  

## HTTP Syntax

`ms.source.uri=https://host-name/path?url-parameters`

For HTTP connections, `ms.source.uri` accepts a domain or host name, optional path segments, and optional URL 
parameters. All of them can be dynamic, i.e., they can contain DIL variables enclosed with double brackets `{{` and `}}`.

For basic authentication, use the following:
    - `source.conn.username`
    - `source.conn.password`
    - `ms.authentication`

For token based authentication:
    - `ms.authentication`

**Note**: Basic authentication can also be configured as token authentication by concatenating username and password, separated
by a column.

For OAuth2.0 authentication:
    - `ms.authentication`
    - `ms.secondary.input`

For form based authentication:
    - `ms.parameters`
    - `ms.http.request.headers={"Content-Type": "application/x-www-form-urlencoded"}`

## S3 Syntax

`ms.source.uri=https://bucket-name.s3.amazonaws.com/prefix-string`

S3 syntax is similar like HTTP syntax, except the `ms.source.uri` has the bucket name as part of host name, and
instead of URL path, it should have optionally a prefix string. 

For authentication, use the following:
- `source.conn.username=access-key`
- `source.conn.password=secrete-id`

## JDBC Syntax

`ms.source.uri=jdbc:database-type://host-name:port/database-name?configurations`

The database type can be `mysql` or `sqlserver`. 

Configurations are name value pairs, separated by `&` such as `useSSL=true&enabledTLSProtocols=TLSv1.2`. 

For authentication, use the following:
- `source.conn.username`
- `source.conn.password`

## SFTP Syntax

`ms.source.uri=path`
`source.conn.host=host-name`

For SFTP, the host name is specified in `source.conn.host`, and the root path is specified in `ms.source.uri`.

For authentication, use the following:
- `source.conn.username`
- `source.conn.password`

or use the following if private key authentication is required:
- `source.conn.private.key`

## Variables

To make any part of the source URI dynamic, add [variables](../concepts/variables.md) as needed. In runtime,
variables will be replaced with actual values, hence source URI can get different values in different work units. 

[Back to Summary](summary.md#config-source-and-authentication)