# Config an Authentication Job

The purpose of the authentication job is to retrieve an authentication token from the data source, so that
subsequent data extraction jobs can use the token to authenticate with the data source. An authentication job is 
needed mostly in cases where credentials or tokens have to be refreshed in each flow execution, such as in 
the case of OAuth2.0 authentication. 

## Base Configuration

An authentication job is also a normal DIL job, therefore it requires the basic configuration. that includes:
- `source.class=com.linkedin.cdi.source.HttpSource`, assuming OAuth2.0, which serve the tokens through HTTP. 
- `ms.extractor.class=com.linkedin.cdi.extractor.JsonExtractor`, assuming the response is in Json format.
- `converter.classes=org.apache.gobblin.converter.avro.JsonIntermediateToAvroConverter`, assuming the retrieved token will
  be stored in Avro format 

## Job Configuration

Specify the target storage properties:

- Specify the dataset name that will store the token record by setting `extract.table.name`
- Specify where to store the token records through `data.publisher.final.dir`; this is typically a staging area with access controls
  in order to maintain the confidentiality of retrieved tokens.
- Ensure the staging area is cleaned up before each execution, so that old tokens will not get in the way by setting
  `data.publisher.replace.final.dir=true` and `writer.file.path.type=namespace_table`.

Specify the HTTP request properties:

- Specify the 3rd party system to connect to, including the endpoint serving the authentication tokens. This is done
  through `ms.source.uri`. For example: `ms.source.uri=https://oauth2.googleapis.com/token`.
- `ms.http.request.method` should be `POST` in most cases as we are "creating" a new token, but it depends on the 3rd party system.
- Authentication jobs mostly use form based authentication, where clients supply access credentials through a form,
  Form based authentication use `ms.parameters` to supply form entries, such as client ids and secrets, etc.
  See [form based authentication](source-authentication.md#http-syntax).
- The content type is normally "x-www-form-urlencoded"; therefore, normally it requires `ms.http.request.headers={"Content-Type": "application/x-www-form-urlencoded"}`. 

Specify the data processing properties:

- Optionally specify the response schema through `ms.output.schema`
- If any token fields need to be encrypted on storage, they should be in `ms.encryption.fields`. 
  For example: `ms.encryption.fields=["access_token"]` will make the "access_token" field encrypted on storage.
  
## Other Recommended Configuration
- `extract.table.type=APPEND_ONLY`
- `state.store.enabled=false`

When the job succeeds, the authentication token will be stored under the location specified in `data.publisher.final.dir`. 

[Back to Summary](summary.md#configanauthenticationjob)