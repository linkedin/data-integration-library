# ms.extractor.class

**Tags**: 
[conversion](categories.md#conversion-properties)

**Type**: string

**Default value**: blank

**Related**:
- [AvroExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/AvroExtractor.md)
- [JsonExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/JsonExtractor.md)
- [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)
- [FileDumpExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/FileDumpExtractor.md)

## Description

`ms.extractor.class` specifies the extractor class to use for data parsing. 
The choice of extractor is based data format. Currently, DIL designed 4 
classes of extractors.

- **AvroExtractor**: if the incoming data is Avro format
- **CsvExtractor**: if the incoming data is Csv format 
- **JsonExtractor**: if the incoming data is Json format
- **FileDumpExtractor**: if the incoming data is to be dumped to file system without
going through converters and writers. In this case, the incoming data can be any format.

### Example

`ms.extractor.class=com.linkedin.cdi.extractor.AvroExtractor`

[back to summary](summary.md#msextractorclass)
