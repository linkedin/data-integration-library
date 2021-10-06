# ms.target.schema.urn

**Tags**: 
[schema](categories.md#schema-properties),
[conversion](categories.md#conversion-properties),
[source](categories.md#source-properties)

**Type**: string

**Format**: A DataHub URN pointing to a dataset or registered schema definition

**Default value**: blank

**Related**:
- [job property: ms.target.schema.urn](ms.target.schema.urn.md)
- [job property: ms.output.schema](ms.output.schema.md)

## Description

Source schema represents the source data structure. Generally, in a data 
ingestion scenario, the source data will be read in, projected, filtered, and
converted. Source schema can be read from the source, like for JDBC data sources, or parsed
from actual data, like JSON data, or defined as a string, or defined in a metadata
store. `ms.target.schema.urn` address the option that defines source schema in metadata store. 

We generally don't define source schema in schema string format. Instead, we directly
define the [output schema](ms.output.schema.md)
if necessary. 
 
`ms.target.schema.urn` is a URN string of the following forms:

- a **dataset** URN, if the source schema can be represented by a dataset,
the latest schema of the dataset will be read from metadata store,
and then parsed to retrieve fields and types, etc.
- a **registered schema** URN, if the source schema is registered with metadata store in
the form of either a pegasus (PDL) or GraphQL schema.
   
### Example

The following use a pre-defined "registered schema" to represent the 
response structure when calling Google batch upload API.

`ms.source.schema.urn=urn:li:registeredSchema:(PEGASUS,dil-draft-schema/com.linkedin.google.BatchinsertResponse)`

[back to summary](summary.md#mssourceschemaurn)   