# ms.csv.escape.character

**Tags**:
[csv extractor](categories.md#csv-extractor-properties)

**Type**: string

**Default value**: "005c"(\)

## Related 
- [ms.csv.column.header](ms.csv.column.header.md)
- [ms.csv.column.header.index](ms.csv.column.header.index.md)
- [ms.csv.column.projection](ms.csv.column.projection.md)
- [ms.csv.default.field.type](ms.csv.default.field.type.md)
- [ms.csv.quote.character]()
- [ms.csv.separator]()
- [ms.csv.skip.lines](ms.csv.skip.lines.md)

## Description

ms.csv.escape.character specifies how characters can be escaped.
Default is "u005C" (backslash '\'). 
see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

The reason that it is specified as "u005c", instead of "\", is that GaaS cannot handle a property
value "\". If a job is executed in Azkaban only, then this can be just "\".

CSV escape.character can also be specified as a variation of unicode. For example: \ can be 
specified as "u005c". Note: in order to keep this form throughout the job execution until it is
being used by DIL, there should be no backslash before 'u', otherwise, GaaS may not be able to
handle the configuration.   

[back to summary](summary.md#mscsvescapecharacter)
