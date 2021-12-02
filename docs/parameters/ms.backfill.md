# ms.backfill

**Tags**:

**Type**: boolean

**Format**: true/false

**Default value**: false

## Related
- [ms.watermark](ms.watermark.md)
- [ms.work.unit.partition](ms.work.unit.partition.md)
- [ms.secondary.input](ms.secondary.input.md)

## Description

Backfilling is to reprocess a chunk of data in the past that is beyond the look-back process using grace
period. For example, when grace period is 7 days, the flow will look back 7 days in each new execution to reprocess data
since 7 days ago. But when we found data in 3 months ago was wrong, we will need to use backfill.

Backfilling is usually a one-time process, i.e., after backfilling, the flow needs to revert to its normal state. But some
large backfill might need multiple executions. For example, to restate past year's data, we might need to backfill
month by month.

Backfilling will disregard the prior states and high watermarks because it is a onetime process.

Backfilling will be executed in incremental mode always, to avoid wiping out prior data. Generally backfill should update
the newly processed data on top of existing data.

## Example

In a snapshot append flow, we found that there was an issue between 2020-12-01 and 2020-12-15, we can do: 
- `ms.backfill=true`
- `ms.watermark=[{"name": "system","type": "datetime", "range": {"from": "2020-12-01", "to": "2020-12-15"}}]`

Additionally, we can change [ms.work.unit.partition](ms.work.unit.partition.md) to suit the new data range.

Additionaly, we can change [ms.secondary.input](ms.secondary.input.md) using filters to limit the number of work units,
or limit backfilling to certain units. 

After backfilling, revert the properties. 

[back to summary](summary.md#msbackfill)