# ms.csv.escape.character

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md)

## Category

[csv extractor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/csv-extractor-parameters.md)

## Type

string

## Required

No

## Default value

"005c"(\)

## Related 
- [job property: ms.csv.separator](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.separator.md)
- [job property: ms.csv.quote.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.quote.character.md)

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
