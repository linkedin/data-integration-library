# ms.work.unit.min.records

**Tags**: 
[watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: integer

**Default value**: 0

**Related**:
- [job property: ms.work.unit.min.units](ms.work.unit.min.units.md)

## Description

`ms.work.unit.min.records` specifies a minimum number of records that are expected. If the total 
processed rows is less than `ms.work.unit.min.records`, the job will fail, generating an alert.

This can be used when a data ingestion job expects a certain number of records every time.
Setting `ms.work.unit.min.records=1` can detect empty ingestion.

[back to summary](summary.md) 