# Key Concepts

## [Authentication Methods](authentication-method.md)

Each system might require credentials be supplied in different ways. The authentication
methods are very system dependent. DIL generalizes the authentication mechanism so that
many authentication schemes can be configured in similar way. 

## [Backfill](backfill.md)

Backfilling is to reprocess a chunk of data in the past that is beyond the look-back process using grace
period.

## [Encryption Methods](encryption-method.md)

There are several types of encryption, and each can engage different methods. Encryption can be used in the following scenarios:

1. Encrypt credentials, like usernames, passwords, keys, or refresh tokens, in configurations
2. Encrypt confidential data, like access tokens, in data files
3. Encrypt credentials that are to be sent over network for authentication
4. Encrypt data files that are to be sent out to cloud storage

## [ISO Duration](iso-date-interval.md)

ISO Duration is a form to specify time intervals in very compact format. The standard is available [Here: ISO 8601 duration format](https://en.wikipedia.org/wiki/ISO_8601#Durations)

## [Job Pattern](job-type.md)

There are several logical types of Azkaban jobs in 
data integration workflows. These job types are not differentiated by
any particular parameter; **they are configured the same way, and
they look alike**; only they serve different **purposes**. 
They are differentiated here because each of 
them follow certain **patterns**. Understanding these logical types 
of jobs can help configure them quicker. 

## [Json Path](json-path.md)

Json structures are nested by nature. JsonObjects can have children JsonObjects or JsonArrays, and JsonArray
can have JsonObjects or JsonArrays. Each field in a JsonObject is called a path segment, and each element index
in a JsonObject is also called a path segment.

Json Path is a string of segments used to identify a nested element in a Json structure. For JsonObject,
each segment represents a field name of the structure; for JsonArray, each segment represents an index of
a record within the array.

## [Pagination](pagination.md)

Pagination is typically used to fetch a large dataset from cloud over HTTP, where
one fetch can only optimally get a limited chunk of data. In such case,
the data is fetched through a series of pages.

## [Secondary Input](secondary-input.md)

Secondary inputs provides additional directives to job execution, in addition to
the primary inputs of job execution, which is its metadata, i.e, job configurations.

## [Session Control](session-control.md)

Session control is the mechanism to maintain state over a stateless connection. For example, Http connections
are stateless. So data ingestion from API often relies on session control. Session control in DIL has broad
usage. Typical scenarios of using session control are:

- [Asynchronous ingestion](../patterns/asynchronous-ingestion-pattern.md)
- [Pagination](../concepts/pagination.md)
- [Two-step file download ](../how-to/status-check-job.md)

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

