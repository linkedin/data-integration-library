# converter.avro.time.format.md

**Tags**:
[gobblin](categories.md#gobblin-properties)

**Type**: string

**Default value**: none

**Related**:

## Description

`converter.avro.time.format` indicates how time values are formatted in the user data. This property
is used by the JSON to AVRO converter in converting fields of type "time". 

This property accepts multiple formats, separated by comma (,), if time values come in with several forms. 

For example:
- `converter.avro.time.format=HH:mm:ss,HH:mm:ss.000'Z'`

[back to summary](summary.md#essential-gobblin-core-properties)