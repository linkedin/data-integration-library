# ms.watermark

**Tags**: 
[watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: string

**Format**: a JsonArray of JsonObjects

**Default value**: 0

**Related**:

- [job property: ms.parameters](ms.parameters.md).
- [job property: ms.work.unit.partition](ms.work.unit.partition.md)
- [key concept: variable](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variable.md) 
- [key concept: work unit](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/work-unit.md)
- [key concept: watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/watermark.md)

## Description

`ms.watermark` define named watermarks for work unit generation, execution control, 
and incremental processing. DIL supports 2 types of watermarks, `datetime` and `unit`.

- **datetime watermark**: a datetime watermark defines a datetime range. 
- **unit watermark**: a unit watermark defines an array of processing units.

There should be 1 and only datetime watermark. If a datetime watermark is not defined,
DIL will implicitly generate a datetime watermark with a range from 2019-01-01 to current date.

There should be no more than 1 unit watermark.  

This document focuses on the syntax of `ms.watermark` property. 
To understand how watermark controls execution, please read: [key concept: watermark](../concepts/watermark.md).
To understand how work unit works, please read: [key concept: work unit](../concepts/work-unit.md).

### More about Datetime Watermark

A datetime watermark is a reference. It doesn't directly effect or control
job execution. The watermark name and boundaries, low watermark 
and high watermark, can be referenced in [variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variable.md),
which can control execution. 
See [ms.parameters](ms.parameters.md).

A datetime watermark is a range, defined by its `from` and `to` field. The range
can be further partitioned per other configurations. 
See [ms.work.unit.partition](ms.work.unit.partition.md) 

Therefore, a datetime watermark could generate 1 or more mini-watermarks when 
partitioned, and each mini-watermark is mapped to a work unit. Therefore, 
each work unit has its own unique watermark.

The range of a datetime watermark is controlled by a `from` and `to` field, and
each of them can be defined in the following ways:

- **a datetime string**: The string has to be in the format of `yyyy-MM-dd HH:mm:ss.SSSSSSz` 
or `yyyy-MM-ddTHH:mm:ss.SSSSSSz`. For example: "2020-01-01". Hour and below grain
are optional. Timezone is optional, and the default is PST. 
- **-(hyphen)**: Hyphen represents the current date time. It will be converted to 
system date during the work unit generation phase. 
- **PxDTyHzM([ISO 8601 duration format](https://en.wikipedia.org/wiki/ISO_8601#Durations))**:
A ISO duration is interpreted as a datetime 
that is `PxDTyHzM` **preceding** current date time. For example, if the definition
is P1D, then it means a date time value (milliseconds) of 1 day before current
date time (milliseconds). Apparently, hypen (-) is just a shorthand for P0DT0H0M. 

_The `from` value of a datetime watermark is usually static_.
The importance of keeping `from` static is that partitions are generated 
based on it. `from` is part of the signature of all work units if no partitioning;
 `from` will decide the start datetime values of all partitions if the watermark
is partitioned, and those values will be signatures of their corresponding 
work units. 

For example, a monthly-partitioned watermark from **2020-01-01** will generate
partitions, and thus work units, like [2020-01-01, 2020-02-01), [2020-02-01, 2020-03-01),
[2020-03-01, 2020-04-01), and so on. If the from value changed to **2020-01-05**,
partitions will be generated like [2020-01-05, 2020-02-05), [2020-02-05, 2020-03-05),
[2020-03-05, 2020-04-05). Because the start time of partitions is the signature
of the work unit, and it is used to identify the state of the work unit in 
state store, the change of `from`, therefore, totally invalidated all prior
work unit states. 

The `from` can be dynamic through the ISO duration format under 
the following situations:

- You are using daily work unit partitioning, and therefore changing the `from` value
by one or more days will not invalidate work unit state stores.  
- Prior execution states are not important, and you want to keep the watermark 
reference to a small range. In such case, you could define the `from` as something
like `P30D`, which will make the reference timeframe starting from **30 days ago**.

**Alert** whenever `from` is dynamic, there could be excessive state store 
records being generated, because partition signatures are floating. This can 
cause small-file problem when state store is on HDFS.  
 
_On the contrary, `to` value of a datetime watermark is usually dynamic_. Most
commonly, it is "-". The `to` value can be PxD if the reference timeframe has to 
end by certain number of days ago. 

When `from` or `to` are specified using IOS duration format, the actual date time is rounded. 
- PxD will round to day level by truncating hours and below precision
- PxDTyH will round to hour level by truncating minutes and below precision
- PxDTyHzM will round to minute level by truncating seconds and below precision

When `from` or `to` are specified using IOS duration format, it can have an optional timezone code. 
The ISO duration string and timezone code are concatenated by a ".". The timezone codes includes UTC, GMT, Amerca/Los_Angeles etc., for example:
- `P0D.UTC`
- `P0D.America/Los_Angeles`

#### datetime watermark examples

`ms.watermark=[{"name": "system","type": "datetime","range": {"from": "2019-01-01", "to": "-"}}]`

`ms.watermark=[{"name": "system","type": "datetime","range": {"from": "2021-06-01", "to": "P0D"}}]`

`ms.watermark=[{"name": "system","type": "datetime","range": {"from": "2021-06-01", "to": "P1D"}}]`

`ms.watermark=[{"name": "system","type": "datetime","range": {"from": "P7D", "to": "-"}}]`

### More about Unit Watermark

A `unit` watermark defines a list of values that will be used by DIL to
generate work units. 

A `unit` watermark can be defined as a JsonArray. 
For example, `["a", "b", "c"]`.

As a shortcut, a `unit` watermark can also be defined as
a comma separated string, like "a,b,c", which then will be converted
to a JsonArray internally.   

A `unit` watermark name can be referenced as a [variable](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variable.md)
directly. 

#### unit watermark examples

`ms.watermark = [
{"name": "system","type": "datetime", "range": {"from": "2021-08-21", "to": "-"}}, 
{"name": "bucketId", "type": "unit", "units": "null,0,1,2,3,4,5,6,7,8,9"}]
`  

`ms.watermark = [
{"name": "dateRange","type": "datetime", "range": {"from": "2020-01-01", "to": "P0D"}}, 
{"name": "siteName", "type": "unit", "units": "https://siteA/,https://SiteB/...siteZ"}]
`

[back to summary](summary.md#mswatermark) 