# ms.csv.separator

**Tags**: 
[csv extractor](categories.md#csv-extractor-properties)

**Type**: string

**Default value**: ,(comma)

## Related 
- [ms.csv.column.header](ms.csv.column.header.md)
- [ms.csv.column.header.index](ms.csv.column.header.index.md)
- [ms.csv.column.projection](ms.csv.column.projection.md)
- [ms.csv.default.field.type](ms.csv.default.field.type.md)
- [ms.csv.escape.character](ms.csv.escape.character.md)
- [ms.csv.quote.character]()
- [ms.csv.skip.lines](ms.csv.skip.lines.md)

## Description

ms.csv.separator specifies the delimiter in the source csv data. see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

CSV separator can also be specified as a variation of unicode. For example: tab (\t) can be 
specified as "u0009". Note: in order to keep this form throughout the job execution until it is
being used by DIL, there should be no backslash before 'u', otherwise, GaaS may not be able to
handle the configuration.   

[back to summary](summary.md#mscsvseparator)
