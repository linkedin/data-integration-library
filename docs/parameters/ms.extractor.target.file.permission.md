# ms.extractor.target.file.permission

**Tags**: 
[conversion](categories.md#conversion-properties)

**Type**: string

**Default value**: "755"

**Related**:
- [ms.extractor.target.file.name](ms.extractor.target.file.name.md)

## Description

`ms.extractor.target.file.permission` set file permission when 
FileDumpExtractor is used.

[FileDumpExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/FileDumpExtractor.md) 
extractor will dump the InputStream directly to HDFS as a file
without going through converters or writers.

### Example

`ms.extractor.target.file.permission=750`

[back to summary](summary.md#msextractortargetfilepermission)
