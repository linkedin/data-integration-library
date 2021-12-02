# DIL Job Properties
[Browse Properties by Category](categories.md)

The following are a full list of DIL job properties. Generally DIL properties complement the job properties defined 
in Gobblin core. When there is an exception, i.e. when a property replaces one or more Gobblin properties,
the property document will explain.  

## [ms.abstinent.period.days](ms.abstinent.period.days.md)
Abstinent Period is designed to avoid re-extracting a dataset repeatedly. This is particularly useful
for situations like downloading files in large quantity.

## ms.activation.property

This is an DIL internal property, and it should not be used explicitly in job configurations. </p>

DIL reads activation entries from secondary input and generates work units for each activation entry, and
each of those work units is given a unique activation parameter using ms.activation.property, therefore
the work unit can be uniquely identified. </p>

Value in ms.activation.property is part of the work unit signature.  

## [ms.audit.enabled](ms.audit.enabled.md)

Setting ms.audit.enabled to true will enable outgoing data auditing. Auditing will trace all outgoing data
including parameters and payloads to data lake through Kafka. 

## [ms.authentication](ms.authentication.md)

ms.authentication job property defines the authentication of a request. It works with HTTP protocol only 
for now, but could be used in other protocols. 

## [ms.backfill](ms.backfill.md)

Backfilling is to reprocess a chunk of data in the past that is beyond the look-back process using grace
period. 

## [ms.call.interval.millis](ms.call.interval.millis.md)

ms.call.interval.millis specifies the minimum time elapsed between requests in the pagination process.   
When a page is retrieved sooner than the interval, to avoid QPS violation, the thread will wait until
the interval has passed. 

ms.call.interval.millis works within an executor thread. In cases of parallel execution, where the 
number of executor threads is more than one, ms.call.interval.millis should be multiple of the interval
allowed by the QPS to avoid QPS violations cross threads.  

## [ms.connection.client.factory](ms.connection.client.factory.md)

ms.connection.client.factory allows vendors specify customized connections with proxy or enhanced security.
The default factory is com.linkedin.cdi.DefaultConnectionClientFactory. 

## [ms.csv](ms.csv.md)

[ms.csv](ms.csv.md) defines CSV extraction and conversion parameters. 
It can have the following parameters:

- **linesToSkip**, specifies how many lines of data to skip in the CSV payload.
The linesToSkip need to be more than the columnHeaderIndex. 
For example, if columnHeaderIndex = 0, the number of lines to skip need to be at least 1. 
When the linesToSkip is not set explicitly, and the columnHeaderIndex is set, linesToSkip = columnHeaderIndex + 1.
When neither linesToSkip and columnHeaderIndex are set, linesToSkip = 0.  
If more lines need to be skipped after the header, then set this parameter explicitly.
- **columnHeaderIndex**, specifies the 0-based row index of the header columns if they are available.
CSV files may have 1 or more descriptive lines before the actual data. These descriptive lines, 
including the column header line, should be skipped. 
Note the column header line can be in any place of the skipped lines. 
- **escapeCharacter**, specifies how characters can be escaped. Default is "u005C" (backslash \). 
This can be specified as a variation of unicode without a backslash (\) before 'u'.
For example: \ can be specified as "u005c".
- **quoteCharacter**, specifies how source data are enclosed by columns. Default is double-quote (").
This can be specified as a variation of unicode without a backslash (\) before 'u'.
For example: | can be specified as "u007C".
- **fieldSeparator**, specifies the field delimiter in the source csv data. The default is comma.
This can be specified as a variation of unicode without a backslash (\) before 'u'.
For example: tab (\t) can be specified as "u0009".
- **recordSeparator**, also called line separator, specifies the line or record
delimiter. The default is system line separator. 
This can be specified as a variation of unicode without a backslash (\) before 'u'.
- **columnProjection**, defines how CSV columns should be arranged and filtered after parse,
before being sent to converter and writer to persist. 
This feature is primarily used to extract selected columns from csv source without a header.
Column projection definition is a comma-separated string, where each value is either an 
integer or a range, with each number representing the 0 based index of the field.
Column projection definition is inclusive, i.e., only the selected fields are included
in the final dataset, if a column projection is defined.  
For example, to include the 0th, 2nd, 3rd, and 4th column from a source that has 6 columns, 
set the value to: `"columnProjection": "0,2-4"`
- **defaultFieldType**, specifies a default type to supersede field type inference.
By default, CsvExtractor tries to infer the true type of fields when inferring schema
However, in some cases, the inference is not accurate, and users may prefer to keep all fields as strings.
In this case `"defaultFieldType": "string"`. 
Supported types: string | int | long | double | boolean | float.

## [ms.data.default.type](ms.data.default.type.md)

`ms.data.default.type` provides a way to explicitly specifies data 
types for certain fields. This is necessary when the source data has 
empty fields, like placeholders, and DIL cannot infer its type properly.

## [ms.data.explicit.eof](ms.data.explicit.eof.md)

`ms.data.explicit.eof` specifies whether an explicit EOF record should 
be sent to converter after processing all records. 

## [ms.data.field](ms.data.field.md)

In a nested response, like JSON or Avro, `ms.data.field` specifies
where the core data (payload) is. 

## [ms.derived.fields](ms.derived.fields.md)

Derived Fields are calculated fields that serve critical roles in data ingestion process, such as compaction. This includes, but is not
limited by, the following:

- Convert a date string to a EPOC date value so that downstream compaction/partitioning can use the long EPOC value
- Extract a part of a field to form a primary key or delta key
- Provide a calculated value based on flow execution state or watermark, such as the often used extraction date derived field
- Lift up a nested element in the response to the toplevel and make it a toplevel field because only toplevel fields can be primary keys or delta keys 
- Persist a job execution variable, such as the work unit identifier, into the final dataset 

## [ms.enable.cleansing](ms.enable.cleansing.md)

Schema cleansing replaces special characters in the schema element names based
on a pattern. By default, it will replace all blank spaces, $, and @ to underscores.

## [ms.enable.dynamic.full.load](ms.enable.dynamic.full.load.md)

`ms.enable.dynamic.full.load` enables or disables dynamic full load.
When enabled (default) and `extract.is.full = false`, DIL will dynamically 
perform a full load if it is a SNAPSHOT_ONLY extract or 
if there is no pre-existing watermarks of the job.

Dynamic full load is a DIL [Single Flow](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/single-flow.md) 
feature that aims to alleviate users from coding 2 separate flows, 
one for the full load and one for the incremental load. 

## [ms.enable.schema.based.filtering](ms.enable.schema.based.filtering.md)

`ms.enable.schema.based.filtering` enables or disables schema-based filtering,
or column projection. When enabled, only fields specified schema 
are projected to final dataset. 

## [ms.encryption.fields](ms.encryption.fields.md)

`ms.encryption.fields` specifies a list of fields to be encrypted before
they are passed to converters. 

## [ms.extractor.class](ms.extractor.class.md)

`ms.extractor.class` specifies the extractor class to use for data parsing. 
The choice of extractor is based data format. Currently, DIL designed 4 
classes of extractors.

## [ms.extractor.target.file.name](ms.extractor.target.file.name.md)

`ms.extractor.target.file.name` specify the file name when 
FileDumpExtractor is used. The file name can be specified as a
string container DIL variables.

## [ms.extractor.target.file.permission](ms.extractor.target.file.permission.md)

`ms.extractor.target.file.permission` set file permission when 
FileDumpExtractor is used.

## [ms.extract.preprocessors](ms.extract.preprocessors.md)

`ms.extract.preprocessors` define one or more preprocessor classes that
handles the incoming data before they can be processed by the extractor. 
When input data is compressed or encrypted, the input stream needs to 
be preprocessed before it can be passed to an DIL extractor to parse.  
`ms.extract.preprocessors` is a comma delimited string if there are 
more than 1 preprocessors.

## [ms.extract.preprocessor.parameters](ms.extract.preprocessor.parameters.md)

When a source file is encrypted, it requires credentials to decrypt.
`ms.extract.preprocessor.parameters` defines parameters to pass into the 
preprocessor along with the input. 

## [ms.grace.period.days](ms.grace.period.days.md)

`ms.grace.period.days` addresses the late arrival problem, which is 
very common if the ingestion source is a data warehouse. 
`ms.grace.period.days` defines a Grace Period for incremental extraction, 
and it adds extra buffer to cutoff timestamp during the
incremental load so that more data can be included. 

## [ms.http.conn.max](ms.http.conn.max.md)

`ms.http.conn.max` defines maximum number of connections to keep
in a connection pool. It limits the total connections to an HTTP
server. The default value is 50.

## [ms.http.conn.per.route.max](ms.http.conn.per.route.max.md)

`ms.http.conn.per.route.max` defines maximum number of connections to keep
in a connection pool. It limits the total connections to a particular
path, or endpoint, on the HTTP server. The default value is 20.

## [ms.http.conn.ttl.seconds](ms.http.conn.ttl.seconds.md)

`ms.http.conn.ttl.seconds` defines maximum idle time allowed when there
is no activity on an HTTP connection. When there is no activity after
TTL passed, the connection is disconnected. The default is 10 seconds. 

## [ms.http.request.headers](ms.http.request.headers.md)

`ms.http.request.headers` specifies custom headers including Content-Type that are to be 
included in HTTP requests. 

## [ms.http.request.method](ms.http.request.method.md)

The expected HTTP method to send the requests, decided by the data source.

## [ms.http.response.type](ms.http.response.type.md)

`ms.http.response.type` specifies less common response types in addition to
the default ones "application/json" or "text/csv". 

## [ms.http.statuses](ms.http.statuses.md)

`ms.http.statuses` defines status codes that should be treated as success, 
warning, or error. 

## ms.http.status.reasons

`ms.http.status.reasons` is for future use. 

`http.status.reasons` define reason codes of special meaning in determining
whether a request was a success or failure. For example, when status is 200, but there is a 
reason to indicate the request was not successful, then the status.reason can be set:`{"error": ["not found"]}`.
An HTTP response is considered success if and only if status code is in 
http.statuses.success and reason code is not in http.status.reasons.error.

Currently, we don't allow exceptions being made to revert errors by using reason code.

## [ms.jdbc.schema.refactor](ms.jdbc.schema.refactor.md)

`ms.jdbc.schema.refactor` specifies the function to apply to JDBC schema. 
The choices are `toupper`, `tolower`, or `none`

## [ms.jdbc.statement](ms.jdbc.statement.md)

`ms.jdbc.statement` specifies the SQL statement for data retrieval. The value
can be any validate statement on any JDBC source.

## [ms.kafka.brokers](ms.kafka.brokers.md)

This specifies the Kafka broker host, such as `kafka.corp.com:1234`

## [ms.kafka.clientId](ms.kafka.clientId.md)

This specifies the Kafka client id, such as `dil-audit`

## [ms.kafka.schema.registry.url](ms.kafka.schema.registry.url.md)

`ms.kafka.schema.registry.url` specifies the auditing schema registry URL.

## [ms.kafka.audit.topic.name](ms.kafka.audit.topic.name.md)

`ms.kafka.audit.topic.name` specifies the auditing topic name, where
DIL wil send auditing events to if auditing is enabled.

## [ms.normalizer.batch.size](ms.normalizer.batch.size.md)

`ms.normalizer.batch.size` specifies the batch size for the normalizer converter
to group rows. Setting `ms.normalizer.batch.size` to 1 has special 
effects of condensing a sparse table.

## [ms.output.schema](ms.output.schema.md)

`ms.output.schema` defines the output schema of extractors. Therefore,
it is also the input schema of the first converter. 

## [ms.pagination](ms.pagination.md)

`ms.pagination` defines key pagination attributes. 

## [ms.parameters](ms.parameters.md)

ms.parameter defines a list of named [variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variables.md) 
that can be referenced in other configuration properties using the syntax of double brackets {{variableName}}.

## ms.payload.property

`ms.payload.property` is an internal property that DIL uses to pass payloads to work units, 
and it should not be used explicitly in job configurations.  

## ms.retention

`ms.retention` is designed for future use.

## [ms.s3.list.max.keys](ms.s3.list.max.keys.md)

`ms.s3.list.max.keys` limit the number of keys when doing a "list" operation
on a S3 bucket. 

## [ms.schema.cleansing](ms.schema.cleansing.md)

Schema cleansing replaces special characters in the schema element names based
on a pattern. By default, it will replace all blank spaces, $, and @ to underscores.

## [ms.secondary.input](ms.secondary.input.md)

Secondary inputs provides additional directives to job execution, in addition to
the primary inputs of job execution, which is its metadata, i.e, job configurations. 

## [ms.session.key.field](ms.session.key.field.md)

Session is a state management mechanism over stateless connections.
For example, although Restful API is stateless, data sources can maintain 
a session in backend by a status field, a session cursor, or through 
[pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md).

`ms.session.key.field` specifies the key field in response in order to retrieve the 
status for session control and the condition for termination.

## [ms.sftp.conn.timeout.millis](ms.sftp.conn.timeout.millis.md)

`ms.sftp.conn.timeout.millis` defines maximum allowed inactive time. The default is 60 seconds.

## [ms.source.data.character.set](ms.source.data.character.set.md)

`ms.source.data.character.set` specifies a character set to parse JSON or CSV payload. 
The default source data character set is UTF-8, which should be good for most use cases.

## [ms.source.files.pattern](ms.source.files.pattern.md)

`ms.source.files.pattern` specifies a pattern to filter files from S3 and SFTP sources.

## [ms.source.s3.parameters](ms.source.s3.parameters.md)

`ms.source.s3.parameters` specifies parameters for S3 connection.

## [ms.source.schema.urn](ms.source.schema.urn.md)

Source schema represents the source data structure. Generally, in a data 
ingestion scenario, the source data will be read in, projected, filtered, and
converted. Source schema can be read from the source, like for JDBC data sources, or parsed
from actual data, like JSON data, or defined as a string, or defined in a metadata
store. `ms.target.schema.urn` address the option that defines source schema in metadata store. 

## [ms.source.uri](ms.source.uri.md)

[`ms.source.uri`](ms.source.uri.md) 
defines the integration point, which is called data source for data ingestion or target for data egression. 
It follows the [URI format](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier). 
The only exception is that authority is not supported, because all authority cannot be fit in the URI.

## [ms.ssl](ms.ssl.md)

[`ms.ssl`](ms.ssl.md) defines SSL parameters. 

## [ms.target.schema](ms.target.schema.md)

`ms.target.schema` defines the target schema in a JsonArray string. 
Target schema denotes the schema to be passed to writer, this applies
to situation where the source data are transformed through a converter
or other processes.

## [ms.target.schema.urn](ms.target.schema.urn.md)

Generally, target schema should be specified through target schema URN.
to avoid coding long schema strings.
An URN can point to the schema storage location on DataHub, which is
the only supported schema storage for now.

## [ms.total.count.field](ms.total.count.field.md)

Total Count field directs DIL how to retrieve the expected total row counts. This is important when there are large
volume of data and [pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md) 
is used to retrieve data page by page. In cases of pagination, the expected total row count is one way to indicate
the end of pagination when the actually retrieved total number of rows matches or surpasses the expected total rows.

## [ms.validation.attributes](ms.validation.attributes.md)

`ms.validation.attributes` defines a threshold to mark a job as successful or failed. 
The threshold can be specified as "success" or "failure" thresholds. The former is 
called a "success" rule, and the later is called a "failure" rule. 

This property is required for [InFlowValidationConverter](https://github.com/linkedin/data-integration-library/blob/master/docs/components/InFlowValidationConverter.md), 
which is a validation converter based on simple count comparison.

## [ms.wait.timeout.seconds](ms.wait.timeout.seconds.md)

`ms.wait.timeout.seconds` is one option to control pagination, it specifies
how long the job will wait before the session ending (success or failure) status is met. 

## [ms.watermark](ms.watermark.md)

`ms.watermark` define watermarks for work unit generation, execution control, 
and incremental processing. DIL supports 2 types of watermarks, `datetime` and `unit`.

A datetime watermark is a reference. It doesn't directly effect or control
job execution. The watermark name and boundaries, low watermark 
and high watermark, can be referenced in [variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variable.md), which can 
control execution. 
See [ms.parameters](ms.parameters.md).

A datetime watermark is a range, defined by its `from` and `to` field. The range
can be further partitioned per other configurations. 
See [ms.work.unit.partition](ms.work.unit.partition.md) 

Therefore, a datetime watermark could generate 1 or more mini-watermarks when 
partitioned, and each mini-watermark is mapped to a work unit. Therefore, 
each work unit has its own unique watermark.

A `unit` watermark defines a list of values that will be used by the DIL to
generate work units. The `unit` watermark name can be referenced as a [variable](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variable.md)
directly.

## ms.watermark.groups

`ms.watermark.groups` is an DIL internal property, and it should not be used explicitly in job configurations.

DIL uses this property to pass work unit signature to work units.

**Alert**: setting this in GaaS flowSpecs may cause parsing error.  

## ms.work.unit.scheduling.starttime

`ms.work.unit.scheduling.starttime` is an DIL internal property, and it should not be used explicitly in job configurations.

When [work unit pacing](ms.work.unit.pacing.seconds.md) is enabled, the job planner will pass the scheduled start time to work units
so that work unit can wait for their moment to start.

## [ms.work.unit.min.records](ms.work.unit.min.records.md)

`ms.work.unit.min.records` specifies a minimum number of records that are expected. If the total 
processed rows is less than `ms.work.unit.min.records`, the job will fail, generating an alert.

## [ms.work.unit.min.units](ms.work.unit.min.units.md)

`ms.work.unit.min.units` specify a minimum number of work units required for the job to be successful. 
if the number of work units is smaller than `ms.work.unit.min.units`, the job will fail, sending an 
alert to operations team. 

## [ms.work.unit.pacing.seconds](ms.work.unit.pacing.seconds.md)

`ms.work.unit.pacing.seconds` can spread out work unit execution by adding a waiting time
in the front of each work unit's execution. The amount of wait time is based on the order of
the work units. It is calculated as `i * ms.work.unit.pacing.seconds`, where `i` is the sequence number
of the work unit.

## [ms.work.unit.parallelism.max](ms.work.unit.parallelism.max.md)

`ms.work.unit.parallelism.max` defines maximum possible parallel work 
units that can be processed in one job execution.

## [ms.work.unit.partial.partition](ms.work.unit.partial.partition.md)

`ms.work.unit.partial.partition` specifies whether the last partition of a multi-day partition scheme can be partial. 
If set to true, it allows the last multi-day partition to be partial (partial month or partial week). 

## [ms.work.unit.partition](ms.work.unit.partition.md)

`ms.work.unit.partition` defines how the watermark will be partitioned to form 
work units. When a watermark is partitioned, each partition will be processed as
a work unit. Partitioning, therefore, allows parallel processing. 

# Essential Gobblin Core Properties
The following are Gobblin core properties that are essential to job configuration. This is only a short list,
for a complete list of Gobblin core properties, please refer to Gobblin documentation.

## [converter.avro.date.format](converter.avro.date.format.md)

`converter.avro.date.format` indicates how date values are formatted in the user data. This property
is used by the JSON to AVRO converter in converting fields of type "date".

## [converter.avro.time.format](converter.avro.time.format.md)

`converter.avro.time.format` indicates how time values are formatted in the user data. This property
is used by the JSON to AVRO converter in converting fields of type "time".

## [converter.avro.timestamp.format](converter.avro.timestamp.format.md)

`converter.avro.timestamp.format` indicates how timestamp values are formatted in the user data. This property
is used by the JSON to AVRO converter in converting fields of type "timestamp".

## [extract.table.name](extract.table.name.md)

`extract.table.name` specifies the target table name, not the source table name. This
is a required parameter if the extractor is anything other than the FileDumpExtractor.
Writers and some converters don't work without it.

## [job.commmit.policy](job.commit.policy.md)

`job.commit.policy` specifies how the job state will be committed when some of its tasks failed. Valid values are
"full" or "successful".

## [source.class](source.class.md)
## [converter.class](converter.classes.md)
