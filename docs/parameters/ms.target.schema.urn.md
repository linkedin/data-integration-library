# ms.target.schema.urn

**Tags**: 
[schema](categories.md#schema-properties), 
[conversion](categories.md#conversion-properties)

**Type**: string

**Format**: A DataHub URN pointing to a registered schema definition

**Default value**: blank

**Related**:
- [job property: ms.target.schema](ms.target.schema.md)

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

[back to summary](summary.md#mstargetschemaurn)   