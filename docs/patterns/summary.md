# FLow Design Patterns

## [Asynchronous Ingestion Pattern](asynchronous-ingestion-pattern.md)

Asynchronous ingestion is common on cloud. In an asynchronous ingestion, a request
for data preparation is sent to the provider, and the data requester
can then check back with the provider on status. When the status changes
to ready, the requester can start the data extraction. 

An asynchronous ingestion can have 3, 4, or 5 steps. Different vendors implement
it slightly differently. But at typical implementation would have:

- Start a request for data and retrieve a tracking number like "request id"
- Check status using the request id until it is ready
- Extract the data 

The requester can execute the above process asynchronously because it doesn't 
need to wait for the data once the request is submitted. It can go do other
things, and check status every one a while. As some data providers use
buffers to stage data for extraction, the data preparation process can be 
significant in many cases. So waiting synchronously doesn't make sense
for data extractors. 

Sample applications of this pattern include:
- [Salesforce (SFDC) bulk API ingestion](../sample-configs/sfdc-bulk-api-ingestion.md)
- [Eloqua API ingestion](../sample-configs/eloqua-api-ingestion.md)
 
## [2-step File Download Pattern](two-step-file-download-pattern.md)

In downloading files from cloud (S3, GCS, Azsure) or FTP (SFTP), the preferred
practice is to do it in 2 steps if the number of objects to be downloaded can be 
more than 1. The first step is to list all the files, and save into a staging file;
the second step is to download the files one by one. 

## [2-step Ingestion Pattern](two-step-ingestion-pattern.md)

In ingesting large amount of data from a provider, we often extract an attribute
that has a short list of values, and use that list of values to divide the 
ingestion of the larger dataset. 

DIL support diving large ingestion by [work units](../concepts/work-unit.md). 
In a 2-step ingestion pattern, a list of attributes can be used to create work
units. And each work unit can be tracked individually in state store. 

In this pattern, the first step is to extract a list of values by one or more
attributes that can fairly evenly divide the whole dataset. The second step is
extract the whole dataset based on the values obtained in step 1. 

This pattern has a lot of similarity with the [2-step file download pattern](two-step-file-download-pattern.md)
It is a more general pattern of ingestion from any sources. One typical application is
survey response ingestion. In the survey application, survey responses are often
organized by surveys. Therefore, we can extract a list of survey IDs first, and
then extract the responses for each survey ID.  

The overall design of this pattern is:
- a job that gets a small set of attributes (mostly 1 attribute) with a finite list of
values that can very well evenly divide the whole dataset. The best candidates are
partition keys. Others like index columns are also desirable. If data is stored on the 
source in separate tables of the same structure, the table names.  
- a second job that retrieves the full dataset using job 1 output to create work units. Each
work unit ingests a filtered subset of data. The work units don't have to be executed
in one execution. They can be executed in a series of recurring runs if the number of work
units is high. 

The advantages of using the 2-step method are:

- A large dataset can be ingested in a very controlled fashion
- Failure in one unit of work doesn't impact the overall success, the retrying process
can rerun the work unit alone if it fails. 
- The work units can be ingested in parallel

Sample applications of this pattern include:

- [Qualaroo Survey API Ingestion](../sample-configs/qualaroo-survey-api-ingestion.md)
- [Google Search Console API Ingestion](../sample-configs/google-search-console-api-ingestion.md)
- [Zoom Webinar API Ingestion](../sample-configs/zoom-webinar-api-ingestion.md)
- [Decipher Survey API Ingestion](../sample-configs/decipher-survey-api-ingestion.md)
- [Thematic Survey API Ingestion](../sample-configs/thematic-survey-api-ingestion.md)

 
## [Egression with Validation Pattern](egression-validation-pattern.md)
