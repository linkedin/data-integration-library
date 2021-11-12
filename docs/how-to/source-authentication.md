# How to Config Source and Authentication

In a DIL pipeline, source is the generally used to refer to the data provider or data store.

## Config Source Properties

The first task configuring source is to specify `source.class`: 

- `source.class=com.linkedin.cdi.source.S3SourceV2` if the source is AWS S3
- `source.class=com.linkedin.cdi.source.HttpSource` if the source is an API server, including HTTP web server
- `source.class=com.linkedin.cdi.source.HdfsSource` if the source is on HDFS
- `source.class=com.linkedin.cdi.source.JdbcSource` if the source is an JDBC-supporting database
- `source.class=com.linkedin.cdi.source.SftpSource` if the source is an SFTP server

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

### HTTP Syntax

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

### S3 Syntax

`ms.source.uri=https://bucket-name.s3.amazonaws.com/prefix-string`

S3 syntax is similar like HTTP syntax, except the `ms.source.uri` has the bucket name as part of host name, and
instead of URL path, it should have optionally a prefix string. 

For authentication, use the following:
- `source.conn.username=access-key`
- `source.conn.password=secrete-id`

### JDBC Syntax

`ms.source.uri=jdbc:database-type://host-name:port/database-name?configurations`

The database type can be `mysql` or `sqlserver`. 

Configurations are name value pairs, separated by `&` such as `useSSL=true&enabledTLSProtocols=TLSv1.2`. 

For authentication, use the following:
- `source.conn.username`
- `source.conn.password`

### SFTP Syntax

`ms.source.uri=path`
`source.conn.host=host-name`

For SFTP, the host name is specified in `source.conn.host`, and the root path is specified in `ms.source.uri`.

For authentication, use the following:
- `source.conn.username`
- `source.conn.password`

or use the following if private key authentication is required:
- `source.conn.private.key`

### Variables

To make any part of the source URI dynamic, add [variables](../concepts/variables.md) as needed. In runtime,
variables will be replaced with actual values, hence source URI can get different values in different work units.


## Config Authentication

After the basic source properties are set, it's time to config authentication. 
For that, please follow [Authentication Methods](../concepts/authentication-method.md) for authentication configuration details.

Many data providers use OAuth2.0, which require an authentication job to retrieve a token before the data extraction 
jobs can use the token in subsequent jobs. See [authentication job](authentication-job.md).

The following is a typical OAuth2.0 authentication flow configuration:

- the authentication job retrieves an authentication token from the source, this job doesn't actually extract data. 
- one or more subsequent jobs use the [token based authentication](../concepts/authentication-method.md#bearer-token-credentials) 
  method leveraging the token from the first job through secondary input 

## Config Source and Authentication for Egress

In egression, data is sent out, and a response is returned. The configuration of egression job is
the same as an ingestion job as if it is ingesting the response from the target system; therefore,
above configuration steps apply to both ingestion and egression. Putting it in simple way:  

**egression = ingestion of the response**

### Extra Egress Configurations

In egression, the payload that will be sent out is supplied through [ms.secondary.input](../parameters/ms.secondary.input.md). 
The secondary input type of "payload" indicating that the path contains files to be sent out. 

[Back to Summary](summary.md#config-source-and-authentication)