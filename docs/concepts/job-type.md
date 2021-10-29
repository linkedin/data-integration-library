# Job Patterns

DIL has a multistage architecture,
and a data integration job may be divided into multiple stages,
and each stage is configured as an Azkaban job, and Azkaban 
jobs are orchestrated to make one or more workflows. 

Typically, we have the following logical types of Azkaban jobs in 
data integration workflows. These job types are not differentiated by
any particular parameter; **they are configured the same way, and
they look alike**; only they serve different **purposes**. 
They are differentiated here because each of 
them follow certain **patterns**. Understanding these logical types 
of jobs can help configure them quicker. 

## Authentication Job

This is for tasks like getting a new access token 
or temporary password. The output of authentication job is normally
stored in a restricted storage area, like an HDFS folder accessible only
to the flow proxy user. A dedicated authentication job is needed only
when credentials need to be refreshed in each flow execution. This 
is typically for OAuth2 authentication with limited TTL on access tokens.
For example, Google API and Salesforce API both use OAuth2 
with 1 hour TTL on access token.  

## Initiation Job

This is typically for asynchronous data integration. 
The initiation request sends a set of parameters to the third party system
to request it start an action, like data extraction. The initiation job
normally returns an id/key that can be used in subsequent jobs. For
example, in Eloqua ingestion, an initiation job sends a request to Eloqua
to start an export process, and the return is an export id.    

## Status Checking Job

A status checking job ensures the data is
ready for consumption on the third party system.
This can be used in asynchronous data ingestion and file downloads. 
In the asynchronous ingestion scenario, this job will keep checking status with the 3P system
every once a while (e.g. every 15 minutes) until the status turns to 
ready or timeout. In the file downloading scenario, the status checking
job can keep checking the availability of source data until they are 
present or timeout.  

## Data Extraction Job

A data extraction job dumps data from the third
party system. The extraction job differs from other jobs in that it normally
has one or more converters, and its target is normally the data lake, while 
`initiation` and `authentication` jobs might just write data to a staging 
area. 

## Data Preparation Job

A data preparation job transforms data in ways 
so that it can be processed by subsequent jobs; this is specifically design for 
the egression. DIL as a data integration tool doesn't provide strong 
data transformation features for the ingestion scenarios. Ingested data can go
through transformation using other tools more conveniently. The data preparation
job only provide very limited transformation that is absolutely needed for
the egression. Tasks like group data by batches and wrapping data with some top
tier indications are typical when sending data out.   

## Data Egression Job

A data egression job sends data to third party systems
and retrieves the response from the third party system. An `egression` job would typically 
take a payload from the secondary input. 

## Validation Job

A validation job can compare data from the primary input and
from the secondary input. The comparison normally happens in a converter. A `validation`
job fails when validation fails.

[Back to Summary](summary.md#job-pattern) 