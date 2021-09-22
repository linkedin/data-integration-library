# ms.csv.quote.character

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md)

## Category

[csv extractor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/csv-extractor-parameters.md)

## Type

string

## Required

No

## Default value

" (double-quote)

## Related 
- [job property: ms.csv.separator](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.separator.md)

## Description


ms.csv.quote.character specifies how source data are enclosed by columns.
Default is double-quote.
see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

CSV quote character can also be specified as a variation of unicode. For example: | can be 
specified as "u007C". Note: in order to keep this form throughout the job execution until it is
being used by DIL, there should be no backslash before 'u', otherwise, GaaS may not be able to
handle the configuration.
