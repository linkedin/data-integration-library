# How to Partition a Pipeline

With large datasets, data pipelines, both ingestion and egression pipelines, often have to be broken into smaller
chunks. This technique is often referred to as partitioning; however, this should not be confused with
dataset partitioning on storage.

Pipeline partitioning leverages work units. A work unit carries out small chunk of a bigger task. For example,
to ingest 100 GB data, we can create 100 work units, and each ingest 1 GB data.

There are 2 prerequisites in order to be able to partition a pipeline:

- The data can be broken into chunks. For example, if an API doesn't provide any ways to break data into chunks, then
the pipeline cannot be partitioned.
- Each chunk can be independently processed. This important for not only parallel processing but also data integrity.
Under the hood, when work units are scheduled, we don't enforce any order of processing.  

We have 2 ways to break a task into chunks, horizontally and vertically. They are both optional, you can use either one
or both of them.

- Horizontally, we can break the data by time if the data has any time attribute(s) that can be filtered upon. 
  - The start of horizontal partitioning is to define a [partitioned watermark](../concepts/watermark.md). 
    A partitioned watermark provides a mechanism to generate one or more time partitions, which are ranges.
  - After that, [variables](../concepts/variables.md) can be defined to format the ranges in proper format. See [ms.parameters](../parameters/ms.parameters.md).
- Vertically, we can slice the data by one or more attributes
  - Vertical partitioning also uses variables. For example, if we are to ingest Google Search Console data for a list
    of domains, we can define a variable on the domains. And behind the scene, DIl will create 1 work unit for each value
    in the list. 
  - There are 2 mechanisms to define variables. One is through the secondary input, the other is through the unit watermark. 
    - The [secondary input](../concepts/secondary-input.md) can provide one or more variables in a tabular file. The fields in the tabular file will be read
      in to make corresponding variables. For example, if a file has country and category fields, the secondary input module
      will produce 2 variables named by the field name. Each row in the file will be mapped to a work unit. 
    - The [unit watermark](../concepts/watermark.md) can provide one variable based on a list of values. Each value in the list will be mapped to a work unit.
    - Currently, only one of the above mechanisms can be used in a job, they cannot be used together.

## Horizontal Partitioning

The steps to partition a pipeline horizontally are as the following:

1. Define a watermark, for the overall range and for the grain of partitioning
2. Define variables based on the watermark 
3. Use the variables in the requests

For example, the following define a weekly partitioned watermark from 2018-01-01 to the current time (P0D), 
and then pull the data week by week. In this case, the use of the variables in implicit because it is a GET method. 
Variables defined in "ms.parameters" are appended to the URL using the "equal" relation. 

```
ms.watermark=[{"name": "system","type": "datetime", "range": {"from": "2018-01-01", "to": "P0D"}}]
ms.work.unit.partition=weekly
ms.parameters=[{"name":"fromDateTime","type":"watermark","watermark":"system","value":"low","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"},{"name":"toDateTime","type":"watermark","watermark":"system","value":"high","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"}]
ms.source.uri=https://api.gong.io/v2/calls
ms.http.request.method=GET
```

The above is equivalent to the following.

```
ms.watermark=[{"name": "system","type": "datetime", "range": {"from": "2018-01-01", "to": "P0D"}}]
ms.work.unit.partition=weekly
ms.parameters=[{"name":"fromDateTime","type":"watermark","watermark":"system","value":"low","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"},{"name":"toDateTime","type":"watermark","watermark":"system","value":"high","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"}]
ms.source.uri=https://api.gong.io/v2/calls?fromDateTime={{fromDateTime}},toDateTime=={{toDateTime}}
ms.http.request.method=GET
```

When use the variables explicitly, the variable names can be free style. For example:

```
ms.watermark=[{"name": "system","type": "datetime", "range": {"from": "2018-01-01", "to": "P0D"}}]
ms.work.unit.partition=weekly
ms.parameters=[{"name":"start","type":"watermark","watermark":"system","value":"low","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"},{"name":"end","type":"watermark","watermark":"system","value":"high","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mm:ss'Z'"}]
ms.source.uri=https://api.gong.io/v2/calls?fromDateTime={{start}},toDateTime=={{end}}
ms.http.request.method=GET
```

## Vertical Partitioning

The steps to partition a pipeline vertically is as the following:

1. Define a unit watermark, or a secondary input
2. Use the variables in the request

For example, the following pull data by custom Ids:

```
ms.watermark=[{"name":"system","type":"datetime","range":{"from":"2021-01-01","to":"P0D"}},{"name":"customerId","type":"unit","units":"64213,73280,10727"}]
ms.source.uri=https://googleads.googleapis.com/v8/customers/{{customerId}}/googleAds:search
```

Another example using the secondary input for vertical partitioning, and time watermark for horizontal partitioning. 
Here the secondary input file is an Avro file with a list of survey Ids. The Avro file can have many fields, but 
we only use the "surveyId" field. 

```
ms.secondary.input=[{"path": "${output.base.dir}/${extract.namespace}/${preceding.table.name}", "fields": ["surveyId"]}]
ms.watermark=[{"name": "system","type": "datetime", "range": {"from": "2021-07-12", "to": "-"}}]
ms.parameters=[{"name":"startDate","type":"watermark","watermark":"dateRange","value":"low","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mmZ"}, {"name":"endDate","type":"watermark","watermark":"dateRange","value":"high","format":"datetime","pattern":"yyyy-MM-dd'T'HH:mmZ"}]
ms.source.uri=https://li.decipherinc.com/api/v1/surveys/{{surveyId}}/data?format=csv&start={{startDate}}&end={{endDate}}
```

## Over Partitioning

Partitioning will lead to many work units being generated, i.e., one work unit for each partition. In cases where
both horizontal and vertical partitioning are used, there could be many work units.

Excessive number of work units could jam the server because Gobblin pre-generate all work units before delegating them
to executors. 

The following guidelines can help handle the situation.

1. Avoid generating too many partitions by controlling the grain and range of horizontal partitioning, and the number of values in vertical
   partitioning. 
2. Use [ms.work.unit.parallelism.max](../parameters/ms.work.unit.parallelism.max.md) to control the number of active work units
   in each execution. But this often has to use [ms.abstinent.period.days](../parameters/ms.abstinent.period.days.md) so that
   the rest partitions can be process in the next execution. 
3. Use piece meal [bootstrap](bootstrap.md) to ingest large dataset initially.   


[Back to Summary](summary.md#how-to-partition-a-pipeline)