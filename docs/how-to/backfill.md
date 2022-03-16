# How to Backfill

Backfilling is to reprocess a chunk of data in the past that is beyond the look-back process using grace
period. Backfill works for incrementally processed flows; there is no need for backfill in snapshot only flows.

## To Backfill a Recent Period

If data in a recent period went bad or missing, the quick fix is to set [ms.grace.period.days](../parameters/ms.grace.period.days.md) to
a large enough number to cover that period. 

If after expanding grace period, the number of work units becomes too large, which often happens when watermark is partitioned and there
are unit watermarks, try the following to limit the number of work units in one execution. And repeat for other work units accordingly.

- Use the filters [ms.secondary.input](../parameters/ms.secondary.input.md) to select only a subset of units in each execution
- Only pick a few units in a unit watermark in each execution, see [ms.watermark](../parameters/ms.watermark.md)

## To Backfill a Far-back Period

If data in a far-back period went bad or missing, and using grace period extension leads to too many work units or too much 
inefficiency, a former backfill is needed. To carry out a former backfill, set the following:

- `ms.backfill=true`
- set `ms.watermark` to the target period 

If the target period is too wide, it can be broke up to multiple segments, using a piecemeal backfill.  

## Allow New Data Override Older Data

To Allow recently back-filled data override older data in the target dataset, compaction should be set to 
always use the latest data, which can be labeled by a timestamp tied to data ingestion time. 

Typically, we do this to allow newly extracted data take priority over previously extracted data:

```
ms.derived.fields=[{"name": "dilExtractedDate", "formula": {"type": "epoc", "source": "CURRENTDATE"}}]
extract.table.type=SNAPSHOT_APPEND
extract.delta.fields=dilExtractedDate
extract.primary.key.fields=<<primary keys>>
```

With above configuration, because back-filled data will always have a more recent timestamp in `dilExtractedDate`,
the compaction tool wil keep the newer records, and purge the older records.

The caveat is that when there are records deleted in the back-filled data, the deleted records will stay in 
the target dataset. 

[Back to Summary](summary.md#how-to-backfill)