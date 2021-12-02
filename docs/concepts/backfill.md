# Backfill

Backfilling is to reprocess a chunk of data in the past that is beyond the look-back process using grace
period. For example, when grace period is 7 days, the flow will look back 7 days in each new execution to reprocess data
since 7 days ago. But when we found data in 3 months ago was wrong, we will need to use backfill. 

Backfilling is usually a one-time process, i.e., after backfilling, the flow needs to revert to its normal state. But some
large backfill might need multiple executions. For example, to restate past year's data, we might need to backfill 
month by month. 

Backfilling will disregard the prior states and high watermarks because it is a onetime process. 

Backfilling will be executed in incremental mode always, to avoid wiping out prior data. Generally backfill should update 
the newly processed data on top of existing data.

[Back to Summary](summary.md#backfill)