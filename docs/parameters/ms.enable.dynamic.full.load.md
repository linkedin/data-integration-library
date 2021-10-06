# ms.enable.dynamic.full.load

**Tags**: 
[execution](categories.md#execution-properties)

**Type**: boolean

**Default value**: true

**Related**:

## Description

`ms.enable.dynamic.full.load` enables or disables dynamic full load.
When enabled (default) and `extract.is.full = false`, DIL will dynamically 
perform a full load if it is a SNAPSHOT_ONLY extract or 
if there is no pre-existing watermarks of the job.

Dynamic full load is a DIL [Single Flow](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/single-flow.md) 
feature that aims to alleviate users from coding 2 separate flows, 
one for the full load and one for the incremental load. 

When `extract.is.full` is true, `ms.enable.dynamic.full.load` has no use.

To use DIL single flow architecture, set `extract.is.full` to **false** explicitly, 
or not setting it and let it use the default value (false). You don't need to
set `ms.enable.dynamic.full.load`, as its default is true. 
  

### Example

`ms.enable.dynamic.full.load=true`

[back to summary](summary.md#msenabledynamicfullload)

