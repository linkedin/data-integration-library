# ms.work.unit.partition

**Tags**: [watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: string

**Format**: A string of one of the values: monthly, weekly, daily, hourly, and none, or a JsonObject.

**Default value**: blank (equivalent to `none`)

**Related**:
 
- [job property: ms.watermark](ms.watermark.md)

## Description

ms.work.unit.partition defines how the watermark will be partitioned to form 
work units. When a watermark is partitioned, each partition will be processed as
a work unit. Partitioning, therefore, allows parallel processing. 

This property supports **plain partitioning** or **structured partitioning**. 

### Plain Partitioning

In **plain partitioning**, the whole watermark is evenly partitioned. In this case, 
the partitioning scheme can be one of the values from `monthly`, `weekly`, `daily`, `hourly`,
and `none`. For example, if you have a job with data range 01/01-03/15, and you want each work 
unit to take a week load of data, set this value to `weekly`.	

More information about the various partition schemes:

- **monthly**: the [watermark](ms.watermark.md)
is partitioned by months from the starting date of the watermark.
For example, if watermark starts from Jan-5, then the first partition is 
Jan-5 (milliseconds of midnight on Jan-5 inclusive)
to Feb-5 (milliseconds of midnight Feb-5 exclusive), and so on. 
It is recommended to pick a start date at the month beginning. If a month end
date is picked as watermark start date, then the partitioning results can be wacky. 

- **weekly**: the [watermark](ms.watermark.md)
is partitioned by weeks from the starting day of the watermark.
For example, if watermark starts from a Monday, then the first partition is 
Monday (milliseconds of midnight on Monday inclusive)
to the next Monday (milliseconds of midnight on next Monday exclusive), and so on.
The watermark start date can be a date that falls on any day.  

- **daily**: the [watermark](ms.watermark.md)
is partitioned by days from the starting date of the watermark, with 
each partition starting from a date (milliseconds of midnight of the date inclusive)
to the next date (milliseconds of midnight of next date exclusive), and so on.

- **hourly**: the [watermark](ms.watermark.md)
is partitioned by hours from the starting date-time of the watermark, with 
each partition starting from a date-time (milliseconds of the date-time inclusive)
to the next date-time an hour away (milliseconds of the next date-time exclusive), and so on.

- **none**: the watermark is not partitioned. 

### Structured Partitioning

In **structured partitioning**, ms.work.unit.partition is a JsonObject, 
and there can be multiple ways to partition the watermark.

For example, the following will break 2010-2019 by monthly partitions, 
and daily partitions afterwards.
`ms.work.unit.partition={"monthly": ["2010-01-01", "2020-01-01"], "daily": ["2020-01-01": "-"]}`

In such case, the partitions are called composite. For composite partitions to work, 
the ranges should be continuous with no gaps or overlaps. In order to avoid gaps and overlaps, 
one range should end where the next range starts.

**Note** the end of partition accepts "-" as current date, but it doesn't access PxD syntax, 
the reason being a partition range can be broader than watermark range.

For a composite partition, if the range definition is unspecified or invalid, 
then the there is no partitioning, equivalent to ms.work.unit.partition=''

For a composite partition, a range is matched against watermark to define partitions, 
if a range is smaller than full partition range, for example `{"monthly": ["2020-01-01", "2020-01-18"]}`, 
it will still generate a full partition. So to avoid confusion, the range should be, at minimum, 1 partition size. 
That means, a range should at least 1 month for monthly, or at least 1 week for weekly etc.

[back to summary](summary.md)
