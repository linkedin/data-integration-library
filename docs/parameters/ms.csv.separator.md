# ms.csv.separator

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md)

## Category

[csv extractor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/csv-extractor-parameters.md)

## Type

string

## Required

No

## Default value

,(comma)

## Related 
- [job property: ms.csv.quote.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.quote.character.md)

## Description

ms.csv.separator specifies the delimiter in the source csv data. see [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md)

CSV separator can also be specified as a variation of unicode. For example: tab (\t) can be 
specified as "u0009". Note: in order to keep this form throughout the job execution until it is
being used by DIL, there should be no backslash before 'u', otherwise, GaaS may not be able to
handle the configuration.   
