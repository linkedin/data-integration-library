# [Config Source and Authentication](source-authentication.md)

Information about the source, such as the protocol and host name, are specified via [ms.source.uri](../parameters/ms.source.uri.md).
The source URI can contain variables. Using variables makes it dynamic. For example, if a job needs to pull many
entities from the same API endpoint, the entity ID be represented with a variable so that we just need one job configuration,
not many repeating job configurations.

# [Config Data Processing](data-processing.md)

For data ingestion, data processing includes decrypting, uncompressing, and parsing extracted or downloaded data. The parsing
step also includes retrieving metadata in order to decide the next action for pagination and session control.

For data egression, data processing includes reading and formatting payload, and structure the egress plan through proper
pagination.

# [Config Data Transformation](data-conversion.md)

Data conversion for ingestion includes the following two types:
- To create derived fields
- Data format conversion
- Dataset and schema tagging
- Encrypting sensitive information

# [Config Data Persistence](data-persistence.md)

Data persistence includes writer and publisher configuration. Writer writes data into a working directory, and publisher
pushes data to the final storage, such as a data lake.

# [Config an Authentication Job](authentication-job.md)

The purpose of the authentication job is to retrieve an authentication token from the third party system, so that
subsequent data extraction jobs can use the token to authenticate with the third party system. An authentication job is
needed mostly in cases where credentials or tokens have to be refreshed in each flow execution, such as in
the case of OAuth2.0 authentication.

# [Config a Status Checking Job](status-check-job.md)

A status checking job ensures the data is ready for consumption on the third party system.
A status checking job tries to read a small piece of information from the third party system, and it then 
verifies per the given criteria for success or failure. This can be used in asynchronous data ingestion and file downloads.

In the asynchronous ingestion scenario, this job will keep checking status with the third party system
every once a while (e.g., every 15 minutes) until the status turns to ready or timeout. In the file 
downloading scenario, the status checking job can keep checking the availability of source data until they are
present or timeout.

# [How to Bootstrap](bootstrap.md)

Bootstrap is to extract the whole history of a snapshot_append dataset from the source.

# [How to Re-bootstrap](re-bootstrap.md)

Re-bootstrap is to re-extract the whole history of a snapshot_append dataset from the source.

# [How to Backfill](backfill.md)

Backfilling is to reprocess a chunk of data in the past that is beyond the look-back process using grace
period. Backfill works for incrementally processed flows; there is no need for backfill in snapshot only flows.

# [How to Partition a Pipeline](partition.md)

With large datasets, data pipelines, both ingestion and egression pipelines, often have to be broken into smaller 
chunks. This technique is often referred to as partitioning; however, this should not be confused with 
dataset partitioning on storage. 

Pipeline partitioning leverages work units. A work unit carries out small chunk of a bigger task. For example,
to ingest 100 GB data, we can create 100 work units, and each ingest 1 GB data. 






