# ms.s3.list.max.keys

**Tags**: 
[source](categories.md#source-properties)

**Type**: integer

**Default value**: 1000

**Minimum value**: 1

**Related**:

## Description

`ms.s3.list.max.keys` limit the number of keys when doing a "list" operation
on a S3 bucket. 

In retrieve files from S3, DIL will first try listing the keys using the path
from [ms.source.uri](ms.source.uri.md),
DIL will only perform a "download" if there is only 1 key. 

If there are multiple keys from the given location, DIL will just write the
list of keys out, and no download performed.

In order to download multiple files, a "list" job is required to list all 
keys and save them in a list file, then a "download" job to get them one by one,
with each key being processed by 1 work unit. 
See [Two Step File Download](https://github.com/linkedin/data-integration-library/blob/master/docs/patterns/two-step-file-download-pattern.md).  

### Statement of Direction

`ms.s3.list.max.keys` will be merged into `ms.source.s3.parameters`.
  
[back to summary](summary.md#mss3listmaxkeys)