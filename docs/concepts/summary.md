# Key Concepts

## [Authentication Methods](authentication-method.md)

## [Encryption Method](encryption-method.md)

## [ISO Duration](iso-date-interval.md)

## [Job Pattern](job-type.md)

There are several logical types of Azkaban jobs in 
data integration workflows. These job types are not differentiated by
any particular parameter; **they are configured the same way, and
they look alike**; only they serve different **purposes**. 
They are differentiated here because each of 
them follow certain **patterns**. Understanding these logical types 
of jobs can help configure them quicker. 

## [Json Path](json-path.md)

## [Pagination](pagination.md)

Pagination is typically used to fetch a large dataset from cloud over HTTP, where
one fetch can only optimally get a limited chunk of data. In such case,
the data is fetched through a series of pages. 

## [Schema](schema.md)

## [Secondary Input](secondary-input.md)

## [Secret Encryption](secret-encryption.md)

## [Session Control](session-control.md)

## [Single Flow](single-flow.md)

## [Variable](variables.md)

A variable provides dynamic input through substitution to parameters. 

## [Watermark](watermark.md)

There are two types of watermarks:

- time-based LongWatermark
- unit-based UnitWaterMark. 

Time watermark defines a time range, with a `from` datetime and a 
`to` datetime. DIL internally handles time watermark values in milliseconds.

A "Unit" watermark holds individual values, like ids. It is a list of string values.

## [Work Unit](work-unit.md)

"Time" watermarks can generate partitions, and "unit" watermarks have units. 

Time watermark and unit watermark together creates work units, and DIL 
maintains execution state including watermarks for each work unit.

