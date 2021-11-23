# Config Data Processing

For data ingestion, data processing includes decrypting, uncompressing, and parsing extracted or downloaded data. The parsing
step also includes retrieving metadata in order to decide the next action for pagination and session control.

For data egression, data processing includes reading and formatting payload, and structure the egress plan through proper
pagination. 

## Preprocessing

Data stored on cloud are often compressed and encrypted, and some API's can supply data in compressed format to save network bandwidth
and increase throughput. To consume such data, DIL used preprocessing modules. Each job can include one or more preprocessors. 
They are configured through [ms.extract.preprocessors](../parameters/ms.extract.preprocessors.md) and [ms.extract.preprocessor.parameters](../parameters/ms.extract.preprocessor.parameters.md).

For example, the following code can be used for downloading a gzipped file from AWS:
- `ms.extract.preprocessors=com.linkedin.cdi.preprocessor.GunzipProcessor`
- `ms.source.uri=https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-${commoncrawl.publish.period}/warc.paths.gz`
- `ms.extractor.target.file.name=warc.paths.gz`

And, the following code can be used for downloading GPG encrypted files from a SFTP server:
- `ms.extract.preprocessor.parameters={"com.linkedin.cdi.preprocessor.GpgProcessor": {"keystore_path" :"${loc}","keystore_password" : "${passphrase}"}}`
- `ms.extract.preprocessors=com.linkedin.cdi.preprocessor.GpgDecryptProcessor`
- `source.conn.host=files.responsys.net`
- `ms.source.uri=/export/data`

Preprocessing is optional. 

## Data Parsing

Parsing the data is done mostly in the extractor module. Each job need to specify one and only one extractor class through [ms.extractor.class](../parameters/ms.extractor.class.md). 

Extractor class can be one of the following values: 

- `com.linkedin.cdi.extractor.JsonExtractor` if the data extracted/downloaded, after preprocessing if applicable, is of JSON format
- `com.linkedin.cdi.extractor.CsvExtractor` if the data extracted/downloaded, after preprocessing if applicable, is of any delimited text format (CSV, PSV etc)
- `com.linkedin.cdi.extractor.AvroExtractor` if the data extracted/downloaded, after preprocessing if applicable, is of Avro format
- `com.linkedin.cdi.extractor.FileDumpExtractor` if the data extracted/downloaded, after preprocessing if applicable, should be save directly to a storage system without further processing(conversion)

To parse the incoming data, the job might need specify one or more of the following properties:

- [ms.data.field](../parameters/ms.data.field.md) if the actual data is wrapped under a sub-element, for example, `ms.data.field=results` if the actual payload is under the "results" column of the response  
- [ms.total.count.field](../parameters/ms.total.count.field.md) if the total row count information is available under the specific field, for example, `ms.total.count.field=records.totalRecords`
- [ms.session.key.field](../parameters/ms.session.key.field.md) if the session key is available under the specific field, for example, `ms.session.key.field="name": "records.cursor"}`
- [ms.pagination](../parameters/ms.pagination.md) if the pagination information like page start, page size, and page number etc are available, for example, `ms.pagination={"fields": ["offset", "limit"], "initialvalues": [0, 25000]}`
- [ms.output.schema](../parameters/ms.output.schema.md) if the data format cannot be reliably inferred from the actual data, for example, `ms.output.schema=[{"columnName":"s3key","isNullable":"true","dataType":{"type":"string"}}]`
- [ms.http.response.type](../parameters/ms.http.response.type.md) if the response from the source system has content-type other than what the extractor is expecting. The
  default expected content-type of `JsonExtractor` is application/json, and the default expected content-type of `CsvExtractor` is application/csv. 

## Schema Cleansing

Incoming data may have schema names that are not supported in downstream processing, including converters or writers. Invalid
characters and white spaces can be replaced with more acceptable characters, such as "_" (underscore). 

For nested data, such as JSON, schema cleansing will go into nested schema and cleanse up to the lowest level. 

Schema cleansing is configured through:

- [ms.schema.cleansing](../parameters/ms.schema.cleansing.md)

## Column Projection

Column projection allows:

- Filter out some unwanted fields
- Reorder output fields

For JSON data, [ms.output.schema](../parameters/ms.output.schema.md) includes fields to be output, and any fields not
in output schema will be excluded if [ms.enable.schema.based.filtering](../parameters/ms.enable.schema.based.filtering.md) is true, which
is the default value. 

For CSV data [ms.csv](../parameters/ms.csv.md) can specify what and how columns should be output. 

[Back to Summary](summary.md#config-data-processing)