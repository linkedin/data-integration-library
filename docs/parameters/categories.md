# Auditing Properties

The following job properties are essential for auditing function:

- [ms.audit.enabled](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.audit.enabled.md)
- [ms.kafka.brokers](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.brokers.md)
- [ms.kafka.clientId](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.clientId.md)
- [ms.kafka.schema.registry.url](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.schema.registry.url.md)
- [ms.kafka.audit.topic.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.audit.topic.name.md)

# Authentication Properties

The following are related to authentication:

- [ms.authentication](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.authentication.md)
- [ms.http.request.headers](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.http.request.headers.md)
- [ms.secondary.input](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.secondary.input.md)
- [ms.Properties](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.Properties.md)
- source.conn.username
- source.conn.password

# Connection Properties

The following are related to connections:

- [ms.connection.client.factory](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.connection.client.factory.md)
- [ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md)
- [ms.source.s3.Properties](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.s3.Properties.md)
- source.conn.username
- source.conn.password

The following could also be related to connections as they define variables:

- [ms.secondary.input](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.secondary.input.md)
- [ms.Properties](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.Properties.md)

# Conversion Properties

The following are related to conversion (converters):

- [ms.data.explicit.eof](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.data.explicit.eof.md)
- [ms.derived.fields](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.derived.fields.md)
- [ms.enable.schema.based.filtering](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.enable.schema.based.filtering.md)
- [ms.encryption.fields](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.encryption.fields.md)
- [ms.extract.preprocessors](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extract.preprocessors.md)
- [ms.extract.preprocessor.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extract.preprocessor.parameters.md)
- [ms.extractor.class](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extractor.class.md)
- [ms.extractor.target.file.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extractor.target.file.name.md)
- [ms.extractor.target.file.permission](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extractor.target.file.permission.md)
- [ms.normalizer.batch.size](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.normalizer.batch.size.md)
- [ms.output.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.output.schema.md)
- [ms.source.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.schema.urn.md)
- [ms.target.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.md)
- [ms.target.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.urn.md)
- [ms.validation.attributes](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.validation.attributes.md)
- ms.converter.csv.max.failures
- ms.converter.keep.null.strings

# CSV Extractor Properties
- [ms.csv.column.header](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.header.md)
- [ms.csv.column.header.index](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.header.index.md)
- [ms.csv.column.projection](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.projection.md)
- [ms.csv.default.field.type](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.default.field.type.md)
- [ms.csv.escape.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.escape.character.md)
- [ms.csv.quote.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
- [ms.csv.separator](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
- [ms.csv.skip.lines](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.skip.lines.md)

# Execution Properties
- [ms.enable.dynamic.full.load](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.enable.dynamic.full.load.md)

# HTTP Properties

The following are related to HTTP sources:

- [ms.authentication](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.authentication.md)
- [ms.http.request.headers](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.http.request.headers.md)
- [ms.http.request.method](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.http.request.method.md)
- [ms.http.response.type](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.http.response.type.md)
- [ms.http.statuses](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.http.statuses.md)

# Pagination Properties 
- [ms.call.interval.millis](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.call.interval.millis.md)
- [ms.pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.pagination.md)
- [ms.session.key.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.session.key.field.md)
- [ms.wait.timeout.seconds](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.wait.timeout.seconds.md)

# Schema Properties

The following are related to schema:

- [ms.data.default.type](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.data.default.type.md)
- [ms.output.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.output.schema.md)
- [ms.source.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.schema.urn.md)
- [ms.target.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.md)
- [ms.target.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.urn.md)
- [ms.schema.cleansing](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.schema.cleansing.md)
- [ms.enable.cleansing](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.enable.cleansing.md)
- [ms.enable.schema.based.filtering](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.enable.schema.based.filtering.md)
- [ms.jdbc.schema.refactor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.jdbc.schema.refactor.md)
- [ms.kafka.schema.registry.url](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.kafka.schema.registry.url.md)
- ms.converter.keep.null.strings

# Source Properties

- [ms.data.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.data.field.md)
- [ms.jdbc.statement](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.jdbc.statement.md)
- [ms.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.parameters.md)
- [ms.s3.list.max.keys](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.s3.list.max.keys.md)
- [ms.session.key.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.session.key.field.md)
- [ms.source.data.character.set](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.data.character.set.md)
- [ms.source.files.pattern](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.files.pattern.md)
- [ms.source.s3.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.s3.parameters.md)
- [ms.source.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.schema.urn.md)
- [ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md)
- [ms.total.count.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.total.count.field.md)
- [ms.wait.timeout.seconds](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.wait.timeout.seconds.md)

# Watermark Work Unit Properties

The following are related to watermarks and work units:

- [ms.abstinent.period.days](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.abstinent.period.days.md)
- [ms.grace.period.days](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.grace.period.days.md)
- [ms.secondary.input](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.secondary.input.md)
- [ms.watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.watermark.md)
- [ms.work.unit.min.records](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.min.records.md)
- [ms.work.unit.min.units](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.min.units.md)
- [ms.work.unit.pacing.seconds](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.pacing.seconds.md)
- [ms.work.unit.parallelism.max](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.parallelism.max.md)
- [ms.work.unit.partial.partition](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.partial.partition.md)
- [ms.work.unit.partition](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.work.unit.partition.md)

