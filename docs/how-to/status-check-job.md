# Config a Status Checking Job

A status checking job ensures the data is ready for consumption on the third party system.
A status checking job tries to read a small piece of information from the third party system, and it then
verifies per the given criteria for success or failure. This can be used in asynchronous data ingestion and file downloads.

In the asynchronous ingestion scenario, this job will keep checking status with the third party system
every once a while (e.g., every 15 minutes) until the status turns to ready or timeout. In the file
downloading scenario, the status checking job can keep checking the availability of source data until they are
present or timeout.

## Base Configuration

Just like other jobs, the base configurations include source, 
authentication, and others in order to retrieve the needed
status information.

Typically, the job would have:

1. `extract.table.type=snapshot_only`
2. `data.publisher.final.dir=<<job_staging_area>>`
3. `state.store.enabled=false`

## Session Control

Status checking can be achieved through session control. 
See [ms.session.key.field](../parameters/summary.md#mssessionkeyfield).
This way of configuration is most commonly used in [asynchronous ingestion](../patterns/summary.md#asynchronous-ingestion-pattern). 

For example, the following uses the "status" field in the response 
return from the third party system as a session key. It expects the 
status to have a value "success" or "ready" when data is ready on the system
for consumption. 

- `ms.session.key.field={"name": "status", "condition": {"regexp": "success|ready"}}`

When the status is not "success" nor "ready", the session 
will keep going until [time out](../parameters/summary.md#mswaittimeoutseconds).
When the session is live, the job will `paginate` by sending requests to the third
party system on [intervals](../parameters/summary.md#mscallintervalmillis).     

Another example, the following uses the "status" field in the "result" field (nested)
in the response from the third party system as the session key. 
The job expects a value "complete" for success, and "failed" for failure. 
When the success criteria is met, the job will complete successfully. When the
failure criteria is met, the job will fail. Otherwise, the job will keep paginating 
until time out.

- `ms.session.key.field={"name": "result.status", "condition": {"regexp": "^complete$"}, "failCondition": {"regexp": "^failed$"}}`

Triggering by session control is also available in 2-step file down from 
S3 or SFTP. For example, the following wait for today's file to be ready, and
it tries to check it every 5 minutes for up to 4 hours. 

`source.class=com.linkedin.cdi.source.SftpSource`
`job.commit.policy=full`
`ms.extractor.class=com.linkedin.cdi.extractor.JsonExtractor`
`converter.classes=org.apache.gobblin.converter.avro.JsonIntermediateToAvroConverter`
`ms.parameters=[{"name":"dt","type":"watermark","watermark":"system","value":"high","format":"datetime","pattern":"yyyyMMdd"}]`
`ms.output.schema=[{"columnName":"values","isNullable":"true","dataType":{"type":"string"}}]`
`ms.call.interval.millis=300000`
`ms.wait.timeout.seconds=14400`
`ms.session.key.field={"name": "values", "condition": {"regexp": "^.*txt.gpg$"}}`
`ms.source.uri={{fileDirectory}}*{{dt}}*.txt.gpg`

## Minimum Record Validation

Status checking can be achieved through minimum record count validation. 
See [ms.work.unit.min.records](../parameters/summary.md#msworkunitminrecords).
This way of configuration is most commonly used in [2 step file download](../patterns/summary.md#2-step-file-download-pattern). 

For example, the following job pulls the today's file from an SFTP server, 
and it will fail the work unit if today's file is not present because the
minimum required record is 1. By the Gobblin retry mechanism, 
when the work unit fails, the job will retry 10 times with an exponentially 
growing interval (in between the retries).  

The configuration used the "currentDate"
dynamic variable in the path so that only today's files are listed, and
"currentDate" is defined as a variable that gets value from the high
watermark, which is "P0D", i.e. the current date.  

- `ms.work.unit.min.records=1`
- `ms.watermark=[{"name": "system","type": "datetime", "range": {"from": "P1D", "to": "P0D"}}]`
- `ms.parameters=[{"name":"currentDate","type":"watermark","watermark":"dateRange","value":"high","format":"datetime","pattern":"yyyyMMdd"}]`
- `ms.source.uri=/home/user/download/file_*{{currentDate}}*.txt.gpg`
- `task.maxretries=10`
- `job.commit.policy=full`

