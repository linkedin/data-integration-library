# ms.schema.cleansing

**Tags**: 
[schema](categories.md#schema-properties)

**Type**: string

**Format**: A JsonObjects

**Default value**: "{}" (a blank JsonOjbect)

**Related**:

## Description

Schema cleansing replaces special characters in the schema element names based
on a pattern. By default, it will replace all blank spaces, $, and @ to underscores.

`ms.schema.cleansing` is a JsonObject, and it supports the following elements:

- **enabled** : true|false
- **pattern**: if enabled, it has default value "(\\s|\\$|@)"
- **replacement**: if enabled, it has default value "_"
- **nullable**: whether to force nullability. 

`nullable` has default value "false". 
If true, all fields will be forced to be nullable.
If false, the schema inference will try to detect nullability from samples.

This configuration has no impact on schemas from metadata stores.

If defined, `ms.schema.cleansing` supersedes [ms.enable.cleansing](ms.enable.cleansing.md)
If `ms.schema.cleansing` is not defined, DIL will check `ms.enable.cleansing`.
If `ms.enable.cleansing` is true, DIL will do the default cleansing. 

**Alert**: This feature should be used only on need basis,
for example, where source data element names are un-conforming, such as 
containing spaces, and needed standardization. In large datasets cleansing 
can be expensive. 

### Statement of Direction

`ms.enable.cleansing` will be deprecated.
  
### Examples

The following makes all inferred schema fields nullable. 

- `ms.schema.cleansing={"enabled": "true", "nullable": "true"}`
 
The following additionally replaces "-" (hyphen) with "_"(underscore). 
 
- `ms.schema.cleansing={"enabled": "true", "pattern": "(\\s|\\$|@|-)"}`

[back to summary](summary.md#msschemacleansing)