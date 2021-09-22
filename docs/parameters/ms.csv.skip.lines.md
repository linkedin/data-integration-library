# ms.csv.skip.lines

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md)

## Category

[csv extractor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/csv-extractor-parameters.md)

## Type

integer

## Required

No

## Default value

If csv.column.header is true, the default is column header index + 1, 
otherwise the default is 0.


## Related 
- [job property: ms.csv.quote.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.quote.character.md)

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