# 2-step Ingestion Pattern

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

Here is how to do so.

## Step 1: Retrieve Partitioning Attributes

Step 1 is to configure a job that reads only the partitioning attributes 
from the source, and save the list of values into a file, in avro format.  

### Base Configuration

Just like other jobs, the base configurations include source, 
authentication, and others in order to retrieve the needed
status information.

Following are typical settings and explanation:

- `data.publisher.final.dir=<<job_staging_area>>`
- `data.publisher.replace.final.dir=true`
- `writer.file.path.type=namespace_table`

**Explanation**: This job is auxiliary, hence the results are normally not 
persisted into final storage, but you can also make it into
final storage if needed. In above, `data.publisher.replace.final.dir`
and `writer.file.path.type` settings ensure the staging folder
is cleaned up everytime, so that prior files are not accumulated.    

- `extract.table.type=snapshot_only`
- `state.store.enabled=false`
- `extract.is.full=true`

**Explanation**: For the same reason of being auxiliary, we typically don't want to track incremental
values each time the list is pulled.

### Step-1 Configuration

- `ms.output.schema=xxx`

**Explanation**: give the output an explict schema if needed. This is optional.
If the output is Json format, the schema
inference usually works well because it is normally very simple.  If the source is JDBC, the
schema can be retrieved from the dataset. 

- `extract.table.name=xxx`

**Explanation**: the `extract.table.name` will used in step 2 to create work units.


## Step 2: Ingest the Full Dataset 

Step 2 is to configure a job that reads the values from step 1, and 
initiate a downloading work unit for each of them. If there are 2 or more 
attributes, the combinations of values as listed 
in the step 1 output table are used to create work units.     

### Base Configuration

Just like other jobs, the base configurations include source, 
authentication, and others in order to retrieve the needed
status information.

Following are typical settings and explanation:

- `state.store.enabled=true`

**Explanation**: Enable state store so that work units can be tracked
individually.

### Step-2 Configuration

#### Required Configurations

- `ms.secondary.input=[{"path": "<<step 1 job staging area>>", "fields": ["attribute1", "attribute2"...]}]`

**Explanation**: configure the location to read the list file. The list file can contain
extra fields, but the job can take only the needed ones. This job property also allows
filtering, to include or exclude certain records. After filtering, each record makes a work unit. 
The fields chosen will make the signature of each work unit. See [ms.secondary.input](../parameters/ms.secondary.input.md)  

- `ms.watermark=xxxx`

**Explanation**: each work unit can have watermarks of its own, but the definition of watermark
is the same across all units created by the secondary input. If the definition of watermark
creates time partitions, each unit from secondary input will have the same number of time partitions.
The combinations (matrix) of time partitions and units from secondary input makes the final work units.
See [concept: work unit](../concepts/work-unit.md)  

#### Optional Configurations

- `ms.work.unit.parallelism.max=100`

**Explanation**: optionally can limit the number of work units to be processed in one execution. 
And the step 2 can be repeated to ingest the full dataset in batches of 100 work units each time.

## Sample Applications

Sample applications of this pattern include:

- [Qualaroo Survey API Ingestion](../sample-configs/qualaroo-survey-api-ingestion.md)
- [Google Search Console API Ingestion](../sample-configs/google-search-console-api-ingestion.md)
- [Zoom Webinar API Ingestion](../sample-configs/zoom-webinar-api-ingestion.md)
- [Decipher Survey API Ingestion](../sample-configs/decipher-survey-api-ingestion.md)
- [Thematic Survey API Ingestion](../sample-configs/thematic-survey-api-ingestion.md)


[Back to Pattern Summary](summary.md#2-step-ingestion-pattern)