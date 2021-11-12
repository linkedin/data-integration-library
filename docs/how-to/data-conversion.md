# Config Data Transformation

Data conversion for ingestion includes the following two types:
- To create derived fields
- Data format conversion
- Dataset and schema tagging
- Encrypting sensitive information

## Config Derived Fields

Derived fields are used in the following scenarios:

- Create one or more primary or delta fields for incremental data compaction
- Push global information down to each row to denormalize a data structure
- Pull a nested data element up to top row level so that it can be used as primary or delta field

Derived fields are configured through [ms.derived.fields](../parameters/ms.derived.fields.md).

## Config Data Format Conversion

Data format conversion includes:

- Converting CSV data to JSON
- Converting JSON data to Avro
- Converting rows into batches of rows

Data format conversion are handled by converters, the configuration is [converter.classes](../parameters/converter.classes.md).

Converters are optional, and there could be multiple converters, i.e, the number of converters can be 0 or more.
Typical converters are:
- org.apache.gobblin.converter.avro.JsonIntermediateToAvroConverter
- org.apache.gobblin.converter.csv.CsvToJsonConverterV2
- com.linkedin.cdi.converter.JsonNormalizerConverter
- com.linkedin.cdi.converter.AvroNormalizerConverter  
- org.apache.gobblin.converter.LumosAttributesConverter
- com.linkedin.cdi.converter.InFlowValidationConverter
- org.apache.gobblin.converter.IdentityConverter

Each converter can have its own set of properties.

### CSV to JSON Converter Properties

- [ms.csv](../parameters/ms.csv.md) specifies the CSV attributes like header line position and column projection, etc.

### JSON to Avro Converter Properties

- [converter.avro.date.format](../parameters/converter.avro.date.format.md), optional, only needed if there are "date" type fields
- [converter.avro.time.format](../parameters/converter.avro.time.format.md), optional, only needed if there are "time" type fields
- [converter.avro.timestamp.format](../parameters/converter.avro.timestamp.format.md), optional, only needed if there are "timestamp" type fields

### Normalizer Properties

- [ms.normalizer.batch.size](../parameters/ms.normalizer.batch.size.md), optional
- [ms.data.explicit.eof](../parameters/ms.data.explicit.eof.md), required to be true 

### Data Validation Properties

- [ms.validation.attributes](../parameters/ms.validation.attributes.md)

## Config Dataset and Schema Tagging

The tagging converters tag attributes to the ingested dataset, at the dataset level or field level.

Currently, the following properties are for dataset tagging:
- **extract.primary.key.fields**, one or more fields that can be used as the logical primary key of the dataset. A primary
  key field can be a nested field. 
- **extract.delta.fields**, one of more fields that can be used as the delta key of the newly extracted records so that they
  can be merged with previously extracted records properly. Delta fields need to be of TIMESTAMP or LONG type. When it is LONG type
  the data need to be EPOCH values. 

## Config Field Encryption

Fields that have to stored with encryption for security can be configured through [ms.encryption.fields](../parameters/ms.encryption.fields.md). 

Those fields will be encrypted using Gobblin encryption codec. 

[Back to Summary](summary.md#config-data-transformation)