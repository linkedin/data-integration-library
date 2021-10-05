# ms.target.schema

**Tags**: 
[schema](categories.md#schema-properties), 
[conversion](categories.md#conversion-properties)

**Type**: string

**Format**: A schema definition in the form of a JsonArray of JsonObjects 
with each JsonObject being a column definition. 

**Default value**: blank

**Related**:
- [job property: ms.target.schema.urn](ms.target.schema.urn.md)

## Description

`ms.target.schema` defines the target schema in a JsonArray string.
Target schema denotes the schema to be passed to writer, this applies
to situation where the source data are transformed through a converter
or other processes.

The syntax of the schema string is same as [ms.output.schema](ms.output.schema.md).

### Example

`ms.target.schema=[{"columnName":"record","isNullable":"false","dataType":{"type": "string"}}, {"columnName":"uuid","isNullable":"false","dataType":{"type": "string"}}, {"columnName":"date","isNullable":"false","dataType":{"type": "timestamp"}}, {"columnName":"survey_path","isNullable":"false","dataType":{"type": "string"}}, {"columnName":"dilExtractedDate","isNullable":"false","dataType":{"type": "long"}}, {"columnName":"start_date","isNullable":"true","dataType":{"type": "timestamp"}}, {"columnName":"additionalinfo","isNullable":"false","dataType":{"type":"map", "values": "string"}}]`

[back to summary](summary.md#mstargetschema)   