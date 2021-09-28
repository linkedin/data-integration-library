# ms.extractor.target.file.name

**Tags**: 
[conversion](categories.md#conversion-properties)

**Type**: string

**Default value**: blank

**Accepts DIL Variables**: yes

**Related**:
- [ms.extractor.target.file.permission](ms.extractor.target.file.permission.md)

## Description

`ms.extractor.target.file.name` specify the file name when 
FileDumpExtractor is used. The file name can be specified as a
string container DIL variables. 

[FileDumpExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/FileDumpExtractor.md) 
extractor will dump the InputStream directly to HDFS as a file 
without going through converters or writers.

### Example

`ms.extractor.target.file.name={{s3key}}`

[back to summary](summary.md#msextractortargetfilename)
