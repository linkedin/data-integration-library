# ms.enable.cleansing

**Tags**: 
[schema](categories.md#schema-properties)

**Type**: boolean

**Format**: true/false

**Default value**: true

**Related**:

## Description

Schema cleansing replaces special characters in the schema element names based
on a pattern. By default, it will replace all blank spaces, $, and @ to underscores.

**Alert**: This feature should be used only on need basis,
for example, where source data element names are un-conforming, such as 
containing spaces, and needed standardization. In large datasets cleansing 
can be expensive. 

This configuration has no impact on schemas from metadata stores.

If defined, [ms.schema.cleansing](ms.schema.cleansing.md) 
supersedes `ms.enable.cleansing`
If `ms.schema.cleansing` is not defined, DIL will check `ms.enable.cleansing`.
If `ms.enable.cleansing` is true, DIL will do the [default cleansing](ms.schema.cleansing.md). 

### Statement of Direction

`ms.enable.cleansing` will be deprecated.
  
### Examples

The following disables schema cleansing if `ms.schema.cleansing` is not defined. 

- `ms.enable.cleansing=false`
 
[back to summary](summary.md#msenablecleansing)