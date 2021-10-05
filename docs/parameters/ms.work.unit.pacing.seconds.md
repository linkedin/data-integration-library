# ms.work.unit.pacing.seconds

**Tags**: 
[watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: integer

**Default value**: 0

**Related**:
 
- [job property: ms.work.unit.partition](ms.work.unit.partition.md)

## Description

ms.work.unit.pacing.seconds can spread out work unit execution by adding a waiting time
in the front of each work unit's execution. The amount of wait time is based on the order of
the work units. It is calculated as `i * ms.work.unit.pacing.seconds`, where `i` is the sequence number
of the work unit.

**Note**: this property can be easily used inappropriately. When there are 3600 work units, and 
`ms.work.unit.pacing.seconds=1`, the last work unit will not start processing until 1 hour later,
no matter how fast other work units are processed.

## Example

Assuming there are 100 work units, and we set `ms.work.unit.pacing.seconds=10`, then the second 
work unit will not start processing until 10th second. Therefore, work units are spread out by
10 second gaps.  

[back to summary](summary.md)