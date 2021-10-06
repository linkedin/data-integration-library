# ms.call.interval.millis

**Tags**: 
[pagination](categories.md#pagination-properties)

**Type**: long

**Default value**: 0

## Related 

## Description 

ms.call.interval.millis specifies the minimum time elapsed between requests in the pagination process.   
When a page is retrieved sooner than the interval, to avoid QPS violation, the thread will wait until
the interval has passed. 

ms.call.interval.millis works within an executor thread. In cases of parallel execution, where the 
number of executor threads is more than one, ms.call.interval.millis should be multiple of the interval
allowed by the QPS to avoid QPS violations cross threads.  

APIs might have quota by second and quota by the hour or day. 

[back to summary](summary.md#mscallintervalmillis)

