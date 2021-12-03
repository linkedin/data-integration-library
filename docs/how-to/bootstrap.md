# How to Bootstrap

Bootstrap is to extract the whole history of a snapshot_append dataset from the source.

To -bootstrap the whole history for a snapshot_append flow, use the following example to trigger a full extract:

- Make sure there is no state store history from testing
- Start the flow
- Make sure `extract.is.full` is `false` after the first run 

[Back to Summary](summary.md#how-to-re-bootstrap)