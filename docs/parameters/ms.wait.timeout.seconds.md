# ms.wait.timeout.seconds

**Tags**: 
[pagination](categories.md#pagination-properties),
[back to summary](summary.md)

**Type**: integer (in seconds)

**Default value**: 600 seconds (or 10 minutes)

**Maximum value**: 24 hours (24 * 3600 seconds)

**Related**:

- [job property: ms.call.interval.millis](ms.call.interval.millis.md).
- [job property: ms.session.key.field](ms.session.key.field.md)

## Description

`ms.wait.timeout.seconds` is one option to control pagination, it specifies
how long the job will wait before the session ending (success or failure) status is met. 
 
When there is no total expected-row-count ([ms.total.count.field](ms.total.count.field.md) is blank), 
the pagination will keep looping and waiting until either the session 
ending condition is met or time out.

### Use Case 

In [asynchronous](https://github.com/linkedin/data-integration-library/blob/master/docs/patterns/asynchronous-ingestion-pattern.md)
data extraction, a request is submitted to data source, and the
data source will provide updated status when the request is completed, and data 
are ready for downloading. 
Therefore, the extraction job will keep checking the status after submitting the 
request by intervals defined in [ms.call.interval.millis](ms.call.interval.millis.md).

At the same time, 
[ms.session.key.field](ms.session.key.field.md)
can specify the status code when the request is completed or failed. 
In each check, DIL will compare the retrieved status with the expected status, and 
ends the looping when the status is completed or failed. 

However, DIL will not loop forever, it will timeout if after timeout period none of 
completed or failed status is returned. 

### Example 1 

The following SalesForce status check job will wait for the "JobComplete" signal, which indicating
the request is ready, and it will timeout after 4 hours. 

`ms.session.key.field={"name": "Sforce-Locator", "condition": {"regexp": "JobComplete"}}`

`ms.wait.timeout.seconds=14400`

### Example 2

The following asynchronous data extraction job will wait for the `complete` or `failed` status
for 600 seconds by checking every 1 second. 

`ms.session.key.field={"name": "result.status", "condition": {"regexp": "^complete$"}, "failCondition": {"regexp": "^failed$"}}`

`ms.call.interval.millis=1000`

`ms.wait.timeout.seconds=600`
        
[back to summary](summary.md#mswaittimeoutseconds)   