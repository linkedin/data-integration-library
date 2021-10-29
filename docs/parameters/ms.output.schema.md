# ms.output.schema

**Tags**: 
[schema](categories.md#schema-properties), 
[conversion](categories.md#conversion-properties)

**Type**: string

**Format**: A JsonArray

**Default value**: blank

**Related**:
- [key concept: schema](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/schema.md)

## Description

`ms.output.schema` defines the output schema of extractors. Therefore,
it is also the input schema of the first converter. 

You can leave this as blank, run the job, and DIL will automatically 
infer the schema, and output in the log.
Then, you can copy and paste the schema(Avro-flavor schema) as the value here.

### Examples

`ms.output.schema=[{"columnName":"path","isNullable":"false","dataType":{"type":"string"}}]`
  
[back to summary](summary.md#msoutputschema)