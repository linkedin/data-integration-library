# ms.csv.quote.character

**Tags**:
[csv extractor](categories.md#csv-extractor-properties)

**Type**: string

**Default value**: " (double-quote)

## Related 
- [ms.csv.column.header](ms.csv.column.header.md)
- [ms.csv.column.header.index](ms.csv.column.header.index.md)
- [ms.csv.column.projection](ms.csv.column.projection.md)
- [ms.csv.default.field.type](ms.csv.default.field.type.md)
- [ms.csv.escape.character](ms.csv.escape.character.md)
- [ms.csv.separator]()
- [ms.csv.skip.lines](ms.csv.skip.lines.md)

## Description

`ms.csv.quote.character` specifies how source data are enclosed by columns.
Default is double-quote.
see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

CSV quote character can also be specified as a variation of unicode. For example: | can be 
specified as "u007C". Note: in order to keep this form throughout the job execution until it is
being used by DIL, there should be no backslash before 'u', otherwise, GaaS may not be able to
handle the configuration.

[back to summary](summary.md#mscsvquotecharacter)
