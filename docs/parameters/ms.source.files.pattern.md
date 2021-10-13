# ms.source.files.pattern

**Tags**:
[source](categories.md#source-properties)

**Type**: string

**Format**: A Java regular expression

**Support DIL Variables**: No

**Default value**: blank

**Related**:
- [job property: ms.source.uri](ms.source.uri.md)

## Description

`ms.source.files.pattern` specifies a pattern to filter files from S3 and SFTP sources.

### Statement of Direction

Source file patterns will be moved to [ms.source.uri](ms.source.uri.md), 
which supports DIL variables, for S3 and SFTP. HDFS source has
already been using ms.source.uri to specify file patterns.  

### Examples

To pick only PGP files from the source.

`ms.source.files.pattern=.+\.pgp($|\n)`

To pick only source files that start with a particular prefix:

`ms.source.files.pattern=^2019-01-01.+`

**Note**: `ms.source.files.pattern` meant to provide advanced filtering
          that SFTP or S3 "list" command cannot do by using its own patterns.
          But, currently, this property is only used to indicate whether the
          results of the "list" command should be saved into a file. When 
          `ms.source.files.pattern` is not blank, it indicates there could 
          be multiple files, hence the "list" results should 
          be saved to a file.  

[back to summary](summary.md#mssourcefilespattern)      