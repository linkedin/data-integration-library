# ms.csv.default.field.type

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md)

## Category

[csv extractor](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/csv-extractor-parameters.md)

## Type

string

## Required

No

## Default value

blank

## Related 
- [job property: ms.csv.quote.character](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.csv.quote.character.md)

## Description

ms.csv.default.field.type specifies a default type to supersede field type inference.
 
By default, CsvExtractor tries to infer the true type of fields when inferring schema
However, in some cases, the inference is not accurate, and users may prefer to keep all fields as strings.
In this case `ms.csv.default.field.type = string`

Supported types: string | int | long | double | boolean | float