# ms.source.files.pattern

**Category**: [execution](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/execution-parameters.md)

**Type**: string

**Format**: A Java regular expression

**Support DIL Variables**: No

**Default value**: ".*"

**Related**:
- [job property: ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md)

## Description

`ms.source.files.pattern` specifies a pattern to filter files from S3 and SFTP sources.

### Statement of Direction

Source file patterns will be moved to [ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md), 
which supports DIL variables, for S3 and SFTP. HDFS source has
already been using ms.source.uri to specify file patterns.  

### Examples

To pick only PGP files from the source.

`ms.source.files.pattern=.+\.pgp($|\n)`

To pick only source files that start with a particular prefix:

`ms.source.files.pattern=^2019-01-01.+`

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mssourcefilespattern)      