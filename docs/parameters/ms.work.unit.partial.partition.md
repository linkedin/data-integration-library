# ms.work.unit.partial.partition

**Tags**: [watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: boolean

**Format**: true/false

**Default value**: true

**Related**: 
- [job property: ms.watermark](ms.watermark.md)
- [job property: ms.work.unit.partition](ms.work.unit.partition.md)

## Description

ms.work.unit.partial.partition specifies whether the last partition of a multi-day partition scheme can be partial.
If set to true, it allows the last multi-day partition to be partial (partial month or partial week)

For example, if you watermark date range 01/01-01/16, and set ms.work.unit.partition=weekly, 
then the third partition of the data will be partial from 01/15-01/16. If you want this 
partition of the data to be dropped, set this value to `false`.

[back to summary](summary.md)