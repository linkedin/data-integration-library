# How to Bootstrap

Re-bootstrap is to re-extract the whole history of a snapshot_append dataset from the source. 

To re-bootstrap the whole history for a snapshot_append flow, use the following examples to trigger a full extract:

Option 1: 
- Delete the whole history of state store 
- Restart the flow
- Safety check: Make sure `extract.is.full` is `false` after the execution, then resume incremental execution schedule.

Option 2:
- set the following and restart the flow
- `extract.is.full=true`, explicitly set this to true
- `ms.grace.period.days=2000`, set this to a larger enough value to cover all history
- `ms.work.unit.parallelism.max=5000`, set this to a larger enough value
- `ms.watermark=[{"name": "system","type": "datetime", "range": {"from": "2018-01-01", "to": "2019-01-01"}}]`
- revert the above after the flow finishes

To re-bootstrap a large dataset, use option 2 with a small range to get start, after that, follow [backfill](backfill.md) 
instructions to backfill the rest in chunks. 

[Back to Summary](summary.md#how-to-bootstrap)