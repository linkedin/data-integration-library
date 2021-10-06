# ms.grace.period.days

**Tags**: 
[watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: integer

**Default value**: 0

**Related**:
- [job property: ms.abstinent.period.days](ms.abstinent.period.days.md)

## Description

`ms.grace.period.days` addresses the late arrival problem, which is 
very common if the ingestion source is a data warehouse. 

`ms.grace.period.days` defines a Grace Period for incremental extraction, 
and it adds extra buffer to cutoff timestamp during the
incremental load so that more data can be included. 

Grace Period is for overlapped data extraction, it assumes that the source can 
have late arrivals, which are older data that showed up in source 
after last extract. For example, a record was modified 2 days ago, 
but did not show up in source until today. In such case, if we extract based on 
the record's `last update date`, the last extraction would have missed that record, 
and today's extraction will again miss it if we cut off by last 
extraction time (yesterday).

A grace period is thus subtracted from the cutoff timestamp, allowing us 
move the cutoff time backward by the Grace Period, allowing late arrivals
be captured in incremental extraction. 

`ms.http.grace.period.days` itself alone doesn't change extraction logic, 
it only changes the work unit watermark. Job configuration need to 
use the watermark derived variables in requests properly in order achieve the
goal of reliable incremental extraction. 

### Example

If we set `ms.http.grace.period.days=2`, and we run the data ingestion job
daily, after last extraction yesterday, the cutoff time would be last
ETL time without grace period. With grace period, the cutoff time becomes
`last ETL time - 2 days`. 

That means the work unit watermark becomes [`last ETL time - 2 days`, `current time`). 

If a variable "fromTime" is defined on the watermark (see [ms.parameters](ms.parameters.md)),
it would have the value `last ETL time - 2 days`. 

If the variable is used in request like `http://domain/path?updateDate>={{fromTime}}`,
then the extraction will include data that was updated since `last ETL time - 2 days`.

With grace period, the extraction would only include data that was updated 
since `last ETL time`, and it would potentially miss late arrivals.

[back to summary](summary.md#msgraceperioddays)
