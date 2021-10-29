# ms.source.files.pattern

**Tags**:
[source](categories.md#source-properties)

**Type**: string

**Format**: A Java regular expression

**Support DIL Variables**: No

**Default value**: ".*"

**Related**:
- [job property: ms.source.uri](ms.source.uri.md)

## Description

`ms.source.files.pattern` specifies a pattern to filter files from S3 and SFTP sources.

`ms.source.files.pattern` meant to provide advanced filtering
that SFTP or S3 "list" command cannot do by using its own patterns.

### Statement of Direction

Source file patterns will be moved to [ms.source.uri](ms.source.uri.md), 
which supports DIL variables, for S3 and SFTP. HDFS source has
already been using ms.source.uri to specify file patterns.  

### Examples

To pick only PGP files from the source.

`ms.source.files.pattern=.+\.pgp($|\n)`

To pick only source files that start with a particular prefix:

`ms.source.files.pattern=^2019-01-01.+`

[back to summary](summary.md#mssourcefilespattern)      