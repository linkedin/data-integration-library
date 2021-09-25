# ms.csv.skip.lines

**Tags**:
[csv extractor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/categories.md#csv-extractor-properties)

**Type**: integer

**Default value**:
If csv.column.header is true, the default is column header index + 1, 
otherwise the default is 0.

## Related 
- [ms.csv.column.header](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.header.md)
- [ms.csv.column.header.index](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.header.index.md)
- [ms.csv.column.projection](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.column.projection.md)
- [ms.csv.default.field.type](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.default.field.type.md)
- [ms.csv.escape.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.escape.character.md)
- [ms.csv.quote.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)
- [ms.csv.separator](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/)

## Description

ms.csv.skip.lines is a CsvExtractor property, it specifies how many 
lines of data to skip in the CSV payload. 
see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

If csv.column.header is true, csv.skip.lines will be column header index + 1 by default, 
if more lines need to be skipped after the header, then set this parameter explicitly.

If csv.column.header is false, csv.skip.lines will be 0 by default, if there are
rows to be skipped, then set this parameter explicitly.

If you want to skip the first 100 rows of the source data, then 
set this value to `ms.csv.skip.lines=100`.

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mscsvskiplines)
