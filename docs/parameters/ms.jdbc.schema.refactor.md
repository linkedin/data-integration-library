# ms.jdbc.schema.refactor

**Tags**: 
[schema](categories.md#schema-properties)

**Type**: string

**Format**: one of the values: toupper, tolower, or none

**Default value**: none

**Related**:

## Description

`ms.jdbc.schema.refactor` specifies the function to apply to JDBC schema.
The choices are `toupper`, `tolower`, or `none`

### Examples

The following is a sample of MySQL SELECT statement.

- `ms.jdbc.statement=select * from ${source.entity} limit {{limit}} offset {{offset}}`

[back to summary](summary.md#msjdbcschemarefactor)