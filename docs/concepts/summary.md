# Key Concepts

## [Pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md)

Pagination is typically used to fetch a large dataset from cloud over HTTP, where
one fetch can only optimally get a limited chunk of data. In such case,
the data is fetched through a series of pages. 

## [Variables](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/variables.md)

A variable provides dynamic input through substitution to parameters. 

## [Watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/watermark.md)

There are two types of watermarks:

- time-based LongWatermark
- unit-based UnitWaterMark. 

Time watermark defines a time range, with a `from` datetime and a 
`to` datetime. DIL internally handles time watermark values in milliseconds.

A "Unit" watermark holds individual values, like ids. It is a list of string values.

## [Work Unit](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/work-unit.md)

"Time" watermarks can generate partitions, and "unit" watermarks have units. 

Time watermark and unit watermark together creates work units, and DIL 
maintains execution state including watermarks for each work unit.

## [Secondary Input](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/secondary-input.md)
