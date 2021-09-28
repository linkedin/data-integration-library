# ms.work.unit.min.units

**Tags**: 
[watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: integer

**Default value**: 0

**Related**:
 - [job property: ms.work.unit.min.records](ms.work.unit.min.records.md)
 
## Description

`ms.work.unit.min.units` specify a minimum number of work units required for the job to be successful. 
if the number of work units is smaller than `ms.work.unit.min.units`, the job will fail, sending an 
alert to operations team. 

This is particularly useful when a data ingestion job expects daily files, for example, but there
is no file on one day, then the job will fail, generating a failure email, alerting there is no
file. 

[back to summary](summary.md)