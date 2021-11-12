# Config Data Transformation

Data conversion for ingestion includes the following two types:
- Derived Fields
- Data format conversion

## Config Derived Fields

Derived fields are used in the following scenarios:

- Create one or more primary or delta fields for incremental data compaction
- Push global information down to each row to denormalize a data structure
- Pull a nested data element up to top row level so that it can be used as primary or delta field

Derived fields are configured through [ms.derived.fields](../parameters/ms.derived.fields.md).

## Config Data Format Conversion

Data format conversion 

[Back to Summary](summary.md#config-data-transformation)