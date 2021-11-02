# extract.table.name

**Tags**: 
[gobblin](categories.md#gobblin-properties)

**Type**: string

**Default value**: no default, required unless the extractor is a FileDumpExtractor

**Related**:

## Description

`extract.table.name` specifies the target table name, not the source table name. This
is a required parameter if the extractor is anything other than the FileDumpExtractor. 
Writers and some converters don't work without it. 


[back to summary](summary.md#essential-gobblin-core-properties)
