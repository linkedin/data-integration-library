# Sample 1: S3/CSV Ingestion

This sample Gobblin job configuration demonstrates how to ingest data from AWS S3 and dump to a local HDFS data lake. 

The job can be executed through Gobblin Standalone if parameters are made into a .pull file. It can also be executed on Azkaban when parameters are made into a .job file. 

## Define the ingestion protocol (required)

Source.class is a Gobblin Core parameter. It defines the protocol to be used. In this case, we are using S3. The class for S3 is [S3SourceV2](https://github.com/linkedin/data-integration-library/blob/master/docs/components/S3SourceV2.md). Here V2 means that it uses S3 Version 2 SDK.

> source.class=com.linkedin.cdi.source.S3SourceV2

## Define data source parameters (required)

Next, we will give more specific information about the data source. 

ms.source.uri is DIL parameter. It defines how the source is identified. In this case, `commoncrawl.s3.amazonaws.com` specifies the bucket `commoncrawl` on S3. The rest of the URI specifies the S3 key of the resource (data file).  

> ms.source.uri=https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-43/cc-index.paths.gz

## Define the format of data being ingested (required)

Ms.extractor.class is a DIL parameter. It defines the format of input data stream, and thus specifies which data parser to use extract the metadata and payload from input stream. 

The extractor class for CSV is [CsvExtractor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/CsvExtractor.md). This extractor parses the CSV structure of the payload together with other tasks. 

> ms.extractor.class=com.linkedin.cdi.extractor.CsvExtractor

## Define the data conversion (optional)

Converter.class is a Gobblin Core parameter. It defines a series conversions before data are stored on data lake. In this case, we convert CSV to JSON, and convert to JSON to AVRO.  
 
> converter.classes=org.apache.gobblin.converter.csv.CsvToJsonConverterV2,org.apache.gobblin.converter.avro.JsonIntermediateToAvroConverter

## Define preprocessing (optional)

Because of compression and encryption, payload data, downloaded from data sources, may not be directly parsable. They may have to be unzipped or unencrypted before they can be parsed by extractors.

ms.extract.preprocessors is DIL parameter. It can define one or more preprocessors. In this case, we have only one [GunzipProcessor](https://github.com/linkedin/data-integration-library/blob/master/docs/components/GunzipProcessor.md).   

> ms.extract.preprocessors=com.linkedin.cdi.preprocessor.GunzipProcessor

## Define source data schema (optional)
ms.output.schema is a DIL parameter. It defines the [schema](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/schema.md) of ingested data. In this case, the ingested data contains 1 field of type `string`. The source data actually doesn't have a column name, for CSV ingestion, we can provide custom column names if source doesn't provide. 

Source data schema can be inferred if not provided. CSV fields will be inferred mostly as nullable strings. 

> ms.output.schema=[{"columnName":"path","isNullable":"true","dataType":{"type":"string"}}]

## Define data lake properties of the ingested data (optional)

These Gobblin Core parameters define where target files are published and state stores are kept. In this case, we will keep them all in local file system. 

> fs.uri=file://localhost/
> state.store.fs.uri=file://localhost/
> data.publisher.final.dir=/tmp/gobblin/job-output

These Gobblin Core parameters define how the ingested data to be named. 

> extract.namespace=com.linkedin.test
> extract.table.name=test

These Gobblin Core parameters define how the ingested data to be processed incrementally if repeating ingestion is incremental, i.e., each ingestion has only incremental changes since last ingestion.  

> extract.table.type=SNAPSHOT_ONLY
> extract.is.full=true

## Define target file properties (optional)

These Gobblin Core parameters define how the ingested data are to be stored. 

> writer.destination.type=HDFS
> writer.output.format=AVRO

## Define job properties (required)

Job.name is both a Gobblin Core and Azkaban properties. Job properties are required, and they are specific to your execution environment. In this case, we will execute the job through Gobblin Standalone.

> job.name=testJob

