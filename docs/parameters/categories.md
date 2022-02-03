# Auditing Properties

The following job properties are essential for auditing function:

- [ms.audit.enabled](ms.audit.enabled.md)
- [ms.kafka.brokers](ms.kafka.brokers.md)
- [ms.kafka.clientId](ms.kafka.clientId.md)
- [ms.kafka.schema.registry.url](ms.kafka.schema.registry.url.md)
- [ms.kafka.audit.topic.name](ms.kafka.audit.topic.name.md)

# Monitoring Properties

The following job properties are essential for monitoring:

- [ms.metrics.enabled](ms.metrics.enabled.md)
- [ms.kafka](ms.kafka.md)

# Authentication Properties

The following are related to authentication:

- [ms.authentication](ms.authentication.md)
- [ms.http.request.headers](ms.http.request.headers.md)
- [ms.secondary.input](ms.secondary.input.md)
- [ms.parameters](ms.parameters.md)
- source.conn.username
- source.conn.password

# Connection Properties

The following are related to connections:

- [ms.connection.client.factory](ms.connection.client.factory.md)
- [ms.source.uri](ms.source.uri.md)
- [ms.source.s3.parameters](ms.source.s3.parameters.md)
- [ms.sftp.conn.timeout.millis](ms.sftp.conn.timeout.millis.md)
- [ms.ssl](ms.ssl.md)
- source.conn.username
- source.conn.password

The following could also be related to connections as they define variables:

- [ms.secondary.input](ms.secondary.input.md)
- [ms.parameters](ms.parameters.md)

# Conversion Properties

The following are related to conversion (converters):

- [ms.data.explicit.eof](ms.data.explicit.eof.md)
- [ms.derived.fields](ms.derived.fields.md)
- [ms.enable.schema.based.filtering](ms.enable.schema.based.filtering.md)
- [ms.encryption.fields](ms.encryption.fields.md)
- [ms.extract.preprocessors](ms.extract.preprocessors.md)
- [ms.extract.preprocessor.parameters](ms.extract.preprocessor.parameters.md)
- [ms.extractor.class](ms.extractor.class.md)
- [ms.extractor.target.file.name](ms.extractor.target.file.name.md)
- [ms.extractor.target.file.permission](ms.extractor.target.file.permission.md)
- [ms.normalizer.batch.size](ms.normalizer.batch.size.md)
- [ms.output.schema](ms.output.schema.md)
- [ms.source.schema.urn](ms.source.schema.urn.md)
- [ms.target.schema](ms.target.schema.md)
- [ms.target.schema.urn](ms.target.schema.urn.md)
- [ms.validation.attributes](ms.validation.attributes.md)

# Extractor Properties
- [ms.csv](ms.csv.md)

# Execution Properties
- [ms.enable.dynamic.full.load](ms.enable.dynamic.full.load.md)

# HTTP Properties

The following are related to HTTP sources:

- [ms.authentication](ms.authentication.md)
- [ms.http.request.headers](ms.http.request.headers.md)
- [ms.http.request.method](ms.http.request.method.md)
- [ms.http.response.type](ms.http.response.type.md)
- [ms.http.statuses](ms.http.statuses.md)
- [ms.http.conn.max](ms.http.conn.max.md)
- [ms.http.conn.per.route.max](ms.http.conn.per.route.max.md)
- [ms.http.conn.ttl.seconds](ms.http.conn.ttl.seconds.md)

# Pagination Properties 
- [ms.call.interval.millis](ms.call.interval.millis.md)
- [ms.pagination](ms.pagination.md)
- [ms.session.key.field](ms.session.key.field.md)
- [ms.wait.timeout.seconds](ms.wait.timeout.seconds.md)

# Schema Properties

The following are related to schema:

- [ms.data.default.type](ms.data.default.type.md)
- [ms.output.schema](ms.output.schema.md)
- [ms.source.schema.urn](ms.source.schema.urn.md)
- [ms.target.schema](ms.target.schema.md)
- [ms.target.schema.urn](ms.target.schema.urn.md)
- [ms.schema.cleansing](ms.schema.cleansing.md)
- [ms.enable.cleansing](ms.enable.cleansing.md)
- [ms.enable.schema.based.filtering](ms.enable.schema.based.filtering.md)
- [ms.jdbc.schema.refactor](ms.jdbc.schema.refactor.md)
- [ms.kafka.schema.registry.url](ms.kafka.schema.registry.url.md)
- ms.converter.keep.null.strings

# Source Properties

- [ms.data.field](ms.data.field.md)
- [ms.jdbc.statement](ms.jdbc.statement.md)
- [ms.parameters](ms.parameters.md)
- [ms.s3.list.max.keys](ms.s3.list.max.keys.md)
- [ms.session.key.field](ms.session.key.field.md)
- [ms.source.data.character.set](ms.source.data.character.set.md)
- [ms.source.files.pattern](ms.source.files.pattern.md)
- [ms.source.s3.parameters](ms.source.s3.parameters.md)
- [ms.source.schema.urn](ms.source.schema.urn.md)
- [ms.source.uri](ms.source.uri.md)
- [ms.total.count.field](ms.total.count.field.md)
- [ms.wait.timeout.seconds](ms.wait.timeout.seconds.md)

# Watermark Work Unit Properties

The following are related to watermarks and work units:

- [ms.abstinent.period.days](ms.abstinent.period.days.md)
- [ms.grace.period.days](ms.grace.period.days.md)
- [ms.secondary.input](ms.secondary.input.md)
- [ms.watermark](ms.watermark.md)
- [ms.work.unit.min.records](ms.work.unit.min.records.md)
- [ms.work.unit.min.units](ms.work.unit.min.units.md)
- [ms.work.unit.pacing.seconds](ms.work.unit.pacing.seconds.md)
- [ms.work.unit.parallelism.max](ms.work.unit.parallelism.max.md)
- [ms.work.unit.partial.partition](ms.work.unit.partial.partition.md)
- [ms.work.unit.partition](ms.work.unit.partition.md)

# Gobblin Properties

The following properties are inherited from Gobblin and enhanced with explicitly validation rules.

- [converter.avro.date.format](converter.avro.date.format.md)
- [converter.avro.time.format](converter.avro.time.format.md)
- [converter.avro.timestamp.format](converter.avro.timestamp.format.md)
- [extract.table.name](extract.table.name.md)
- [job.commit.policy](job.commit.policy.md)  
- [source.class](source.class.md)
- [converter.class](converter.classes.md)