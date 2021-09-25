# ms.target.schema.urn

**Tags**: 
[schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/categories.md#schema-properties), 
[conversion](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/categories.md#conversion-properties)

**Type**: string

**Format**: A DataHub URN pointing to a registered schema definition

**Default value**: blank

**Related**:
- [job property: ms.target.schema](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.target.schema.md)

## Description

Target schema denotes the schema to be passed to writer, this applies
to situation where the source data are transformed through a converter
or other processes.

Generally, target schema should be specified through target schema URN.
to avoid coding long schema strings.
An URN can point to the schema storage location on DataHub, which is
the only supported schema storage for now.

### Example

`ms.target.schema.urn=urn:li:registeredSchema:(PEGASUS,draft-schema/com.linkedin.google.UploadClickConversionsRequest)`

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mstargetschemaurn)   