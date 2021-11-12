# Config Data Transformation

Data conversion for ingestion includes the following two types:
- Derived Fields
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

- converting CSV data to JSON
- converting JSON data to Avro
- converting rows into batches of rows

Data format conversion are handled by converters, the configuration is [converter.classes](../parameters/converter.classes.md).

Converters are optional, and there could be multiple converters, i.e, the number of converters can be 0 or more.

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

## Config Dataset and Schema Tagging

## Config Field Encryption

[Back to Summary](summary.md#config-data-transformation)