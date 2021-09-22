# DIL Job Properties

The following are a full list of DIL job properties. Generally DIL properties complement the job properties defined 
in Gobblin core. When there is an exception, i.e. when a property replaces one or more Gobblin properties,
the property document will explain.  

## [ms.abstinent.period.days](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.abstinent.period.days.md)
Abstinent Period is designed to avoid re-extracting a dataset repeatedly. This is particularly useful
for situations like downloading files in large quantity.

## ms.activation.property

This is an DIL internal property, and it should not be used explicitly in job configurations. </p>

DIL reads activation entries from secondary input and generates work units for each activation entry, and
each of those work units is given a unique activation parameter using ms.activation.property, therefore
the work unit can be uniquely identified. </p>

Value in ms.activation.property is part of the work unit signature.  

## [ms.audit.enabled](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.audit.enabled.md)

Setting ms.audit.enabled to true will enable outgoing data auditing. Auditing will trace all outgoing data
including parameters and payloads to data lake through Kafka. 

## [ms.authentication](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.authentication.md)

ms.authentication job property defines the authentication of a request. It works with HTTP protocol only 
for now, but could be used in other protocols. 

## ms.backfill

This is for future back fill automation. It has no use currently.  

## [ms.call.interval.millis](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.call.interval.millis.md)

ms.call.interval.millis specifies the minimum time elapsed between requests in the pagination process.   
When a page is retrieved sooner than the interval, to avoid QPS violation, the thread will wait until
the interval has passed. 

ms.call.interval.millis works within an executor thread. In cases of parallel execution, where the 
number of executor threads is more than one, ms.call.interval.millis should be multiple of the interval
allowed by the QPS to avoid QPS violations cross threads.  

## [ms.connection.client.factory](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.connection.client.factory.md)

ms.connection.client.factory allows vendors specify customized connections with proxy or enhanced security.
The default factory is com.linkedin.cdi.DefaultConnectionClientFactory. 

## ms.converter.csv.max.failures

This is for future CSV converter.

## ms.converter.keep.null.strings

This is for future CSV converter.

## [ms.csv.column.header](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.header.md)

ms.csv.column.header specifies whether the CSV data contains a header row.

## [ms.csv.column.header.index](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.header.index.md)

ms.csv.column.header.index specifies the 0-based row index of the header columns if they are available.

## [ms.csv.column.projection](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.projection.md)

ms.csv.column.projection defines how CSV columns should be arranged and filtered after parse,
before being sent to converter and writer to persist. 

Column projection definition is a comma-separated string, where each value is either an 
integer or a range, with each number representing the 0 based index of the field.

Column projection definition is inclusive, i.e., only the selected fields are included
in the final dataset, if a column projection is defined.  

This feature is primarily used to extract selected columns from csv source without a header.

## [ms.csv.default.field.type](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.default.field.type.md)

ms.csv.default.field.type specifies a default type to supersede field type inference.

By default, CsvExtractor tries to infer the true type of fields when inferring schema
However, in some cases, the inference is not accurate, and users may prefer to keep all fields as strings.
In this case `ms.csv.default.field.type = string`

## [ms.csv.escape.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.escape.character.md)

ms.csv.escape.character specifies how characters can be escaped.
Default is "u005C" (backslash '\'). 
see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

## [ms.csv.quote.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)

ms.csv.quote.character specifies how source data are enclosed by columns.
Default is double-quote. 
see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

## [ms.csv.separator](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)

`ms.csv.separator` specifies the delimiter in the source csv file. 
Default is comma. 
see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

## [ms.csv.skip.lines](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.skip.lines.md)

`ms.csv.skip.lines` is a CsvExtractor property, it specifies how many 
lines of data to skip in the CSV payload. see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)


## [ms.data.default.type](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)


## [ms.data.explicit.eof](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)

## [ms.data.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)

## [ms.derived.fields](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.derived.fields.md)

Derived Fields are calculated fields that serve critical roles in data ingestion process, such as compaction. This includes, but is not
limited by, the following:

- Convert a date string to a EPOC date value so that downstream compaction/partitioning can use the long EPOC value
- Extract a part of a field to form a primary key or delta key
- Provide a calculated value based on flow execution state or watermark, such as the often used extraction date derived field
- Lift up a nested element in the response to the toplevel and make it a toplevel field because only toplevel fields can be primary keys or delta keys 
- Persist a job execution variable, such as the work unit identifier, into the final dataset 

## [ms.enable.cleansing](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.enable.dynamic.full.load](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.enable.schema.based.filtering](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.encoding](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.encryption.fields](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.extractor.class](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.extractor.target.file.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.extractor.target.file.permission](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.extract.preprocessors](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.extract.preprocessor.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.grace.period.days](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.grace.period.days.md)
## [ms.http.request.headers](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.http.request.method](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.http.request.method.md)

The expected HTTP method to send the requests, decided by the data source.

## [ms.http.response.type](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.http.statuses](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.http.status.reasons](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.jdbc.schema.refactor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.jdbc.statement](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.kafka.brokers](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.kafka.clientId](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.kafka.schema.registry.url](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.kafka.audit.topic.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.normalizer.batch.size](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.output.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.pagination.md)
## [ms.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.parameters.md)

ms.parameter defines a list of named [variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variables.md) 
that can be referenced in other configuration properties using the syntax of double brackets {{variableName}}.

## [ms.payload.property]

`ms.payload.property` is an internal property that DIL uses to pass payloads to work units, 
and it should not be used explicitly in job configurations.  

## [ms.retention](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.s3.list.max.keys](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.schema.cleansing](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.secondary.input](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.session.key.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.session.key.field.md)
## [ms.source.data.character.set](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.source.files.pattern](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.source.s3.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
## [ms.source.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.schema.urn.md)

Source schema represents the source data structure. Generally, in a data 
ingestion scenario, the source data will be read in, projected, filtered, and
converted.

Source schema can be read from the source, like for JDBC data sources, or parsed
from actual data, like JSON data, or defined as a string, or defined in a metadata
store. 

`ms.target.schema.urn` address the option that defines source schema in metadata store. 

## [ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md)

ms.source.uri defines the integration point, which is called data source for data ingestion or target for data egression. 
It follows the [URI format](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier). 
The only exception is that authority is not supported, because all authority cannot be fit in the URI.

## [ms.target.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.md)

Target schema denotes the schema to be passed to writer, this applies
to situation where the source data are transformed through a converter
or other processes.

## [ms.target.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.urn.md)

Generally, target schema should be specified through target schema URN.
to avoid coding long schema strings.
An URN can point to the schema storage location on DataHub, which is
the only supported schema storage for now.

## [ms.total.count.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.total.count.field.md)

Total Count field directs DIL how to retrieve the expected total row counts. This is important when there are large
volume of data and [pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md) 
is used to retrieve data page by page. In cases of pagination, the expected total row count is one way to indicate
the end of pagination when the actually retrieved total number of rows matches or surpasses the expected total rows.

## [ms.validation.attributes](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.validation.attributes.md)

`ms.validation.attributes` defines a threshold to mark a job as successful or failed. 
The threshold can be specified as "success" or "failure" thresholds. The former is 
called a "success" rule, and the later is called a "failure" rule. 

This property is required for [InFlowValidationConverter](https://github.com/linkedin/data-integration-library/blob/master/docs/components/InFlowValidationConverter.md), 
which is a validation converter based on simple count comparison.

## [ms.wait.timeout.seconds](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.wait.timeout.seconds.md)

`ms.wait.timeout.seconds` is one option to control pagination, it specifies
how long the job will wait before the session ending (success or failure) status is met. 

## [ms.watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.watermark.md)

`ms.watermark` define watermarks for work unit generation, execution control, 
and incremental processing. DIL supports 2 types of watermarks, `datetime` and `unit`.

A datetime watermark is a reference. It doesn't directly effect or control
job execution. The watermark name and boundaries, low watermark 
and high watermark, can be referenced in [variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variable.md), which can 
control execution. 
See [ms.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.parameters.md).

A datetime watermark is a range, defined by its `from` and `to` field. The range
can be further partitioned per other configurations. 
See [ms.work.unit.partition](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.partition.md) 

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

When [work unit pacing](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.pacing.seconds.md) is enabled, the job planner will pass the scheduled start time to work units
so that work unit can wait for their moment to start.

## [ms.work.unit.min.records](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.min.records.md)

`ms.work.unit.min.records` specifies a minimum number of records that are expected. If the total 
processed rows is less than `ms.work.unit.min.records`, the job will fail, generating an alert.

## [ms.work.unit.min.units](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.min.units.md)

`ms.work.unit.min.units` specify a minimum number of work units required for the job to be successful. 
if the number of work units is smaller than `ms.work.unit.min.units`, the job will fail, sending an 
alert to operations team. 

## [ms.work.unit.pacing.seconds](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.pacing.seconds.md)

`ms.work.unit.pacing.seconds` can spread out work unit execution by adding a waiting time
in the front of each work unit's execution. The amount of wait time is based on the order of
the work units. It is calculated as `i * ms.work.unit.pacing.seconds`, where `i` is the sequence number
of the work unit.

## [ms.work.unit.parallelism.max](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.parallelism.max.md)

`ms.work.unit.parallelism.max` defines maximum possible parallel work 
units that can be processed in one job execution.

## [ms.work.unit.partial.partition](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.partial.partition.md)

`ms.work.unit.partial.partition` specifies whether the last partition of a multi-day partition scheme can be partial. 
If set to true, it allows the last multi-day partition to be partial (partial month or partial week). 

## [ms.work.unit.partition](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.partition.md)

`ms.work.unit.partition` defines how the watermark will be partitioned to form 
work units. When a watermark is partitioned, each partition will be processed as
a work unit. Partitioning, therefore, allows parallel processing. 

# Essential Gobblin Core Properties
The following are Gobblin core properties that are essential to job configuration. This is only a short list,
for a complete list of Gobblin core properties, please refer to Gobblin documentation.

# [source.class](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/source.class.md)
# [converter.class](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/converter.class.md)
