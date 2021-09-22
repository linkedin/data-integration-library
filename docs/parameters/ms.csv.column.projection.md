# ms.csv.column.projection

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

ms.csv.column.projection defines how CSV columns should be arranged and filtered after parse,
before being sent to converter and writer to persist. 

Column projection definition is a comma-separated string, where each value is either an 
integer or a range, with each number representing the 0 based index of the field.

Column projection definition is inclusive, i.e., only the selected fields are included
in the final dataset, if a column projection is defined.  

This feature is primarily used to extract selected columns from csv source without a header.

If you want to include the 0th, 2nd, 3rd, and 4th column from a source that has 6 columns, 
set the value to: `ms.csv.column.projection=0,2-4`

Other examples are:

0,1,2,3,4
0,1,2,4-15
0,1,3-7,10
0,5,3-4,2

**Note**: the values need not be ordered

