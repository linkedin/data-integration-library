# ms.target.schema

**Category**: [execution](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/execution-parameters.md)

**Type**: string

**Format**: A schema definition in the form of a JsonArray of JsonObjects 
with each JsonObject being a column definition. 

**Default value**: blank

**Related**:
- [job property: ms.target.schema.urn](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.urn.md)

## Description

Target schema denotes the schema to be passed to writer, this applies
to situation where the source data are transformed through a converter
or other processes.

The syntax of the schema string is same as [ms.output.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.output.schema.md).

### Example

`ms.target.schema=[{"columnName":"record","isNullable":"false","dataType":{"type": "string"}}, {"columnName":"uuid","isNullable":"false","dataType":{"type": "string"}}, {"columnName":"date","isNullable":"false","dataType":{"type": "timestamp"}}, {"columnName":"survey_path","isNullable":"false","dataType":{"type": "string"}}, {"columnName":"dilExtractedDate","isNullable":"false","dataType":{"type": "long"}}, {"columnName":"start_date","isNullable":"true","dataType":{"type": "timestamp"}}, {"columnName":"additionalinfo","isNullable":"false","dataType":{"type":"map", "values": "string"}}]`

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mstargetschema)   