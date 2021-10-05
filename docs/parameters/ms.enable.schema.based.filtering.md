# ms.enable.schema.based.filtering

**Tags**: 
[conversion](categories.md#conversion-properties)

**Type**: boolean

**Default value**: true

**Related**:

## Description

`ms.enable.schema.based.filtering` enables or disables schema-based filtering,
or column projection. When enabled, only fields specified schema 
are projected to final dataset. 

Each Extractor will enforce a compliance filter based on given schema, 
currently this is soft enforced. Use case can turn the filtering off by 
setting this parameter to false.

Whe a flow uses a normalizer converter, this generally should be disabled.

### Example

`ms.enable.schema.based.filtering=false`

[back to summary](summary.md#msenableschemabasedfiltering)

