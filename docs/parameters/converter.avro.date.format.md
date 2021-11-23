# converter.avro.date.format.md

**Tags**:
[gobblin](categories.md#gobblin-properties)

**Type**: string

**Default value**: none

**Related**:

## Description

`converter.avro.date.format` indicates how date values are formatted in the user data. This property
is used by the JSON to AVRO converter in converting fields of type "date".

This property accepts multiple formats, separated by comma (,), if date values come in with several forms.

For example:
- `converter.avro.date.format=MM/dd/yyyy HH:mm,dd-MMM-yyyy HH:mm:ss`

[back to summary](summary.md#essential-gobblin-core-properties)