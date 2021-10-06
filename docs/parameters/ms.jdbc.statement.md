# ms.jdbc.statement

**Tags**:
[source](categories.md#source-properties) 

**Type**: string

**Default value**: blank

**Related**:

## Description

`ms.jdbc.statement` specifies the SQL statement for data retrieval. The value
can be any validate statement on any JDBC source.

DIL doesn't explicitly restrict or support syntax of the statement. 
The source database decides whether to accept or fail the statement.
   
### Examples

The following is a sample of MySQL SELECT statement.

- `ms.jdbc.statement=select * from ${source.entity} limit {{limit}} offset {{offset}}`

[back to summary](summary.md#msjdbcstatement)