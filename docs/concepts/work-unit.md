# Work Unit

A time partition and an activation unit make a work unit. DIL
maintains execution states, including watermarks, for each work unit.

A "time" watermark can generate partitions. Time partitions are defined by 
[ms.watermark](../parameters/ms.watermark.md) and
[ms.work.unit.partition](../parameters/ms.work.unit.partition.md).

Activation units can have 2 sources: 
- units of a unit watermark defined in [ms.watermark](../parameters/ms.watermark.md)
- activation entries of a secondary input from [ms.secondary.input](../parameters/ms.secondary.input.md)

Partitions and Units make a matrix. Assuming we have m periods and n units, 
the matrix will be n x m. **Each combination of partitions and units makes 
a work unit**. That means there will be n x m work units. 

The following scenarios are possible:

- Only time watermark is defined, no secondary input nor explicit unit 
watermark, and time watermark is **not partitioned**, 
there will be only 1 work unit 
- Only time watermark is defined, no secondary input nor explicit unit 
watermark, and time watermark is partitioned into **m partitions**, 
there will be m work units
- Only unit watermark is defined, either through secondary input or 
through explicit definition, default none-partition time watermark, 
there will be n work units
- Both time watermark and unit watermark are defined, there will 
be n x m work units

In any of above cases, each work unit tracks its own high watermark. 

**Note**: Work units are executed as tasks in runtime. Therefore, tasks and 
work units are exchangeable concepts.

## Related
- [ms.watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.watermark.md)
- [ms.grace.period.days](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.grace.period.days.md)
- [ms.abstinent.period.days](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.abstinent.period.days.md)

[Back to Summary](summary.md#work-unit)