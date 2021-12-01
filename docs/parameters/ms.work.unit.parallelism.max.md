# ms.work.unit.parallelism.max

**Tags**: [watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: integer

**Default value**: 500 (value 0 will also be interpreted as 500)

**Max value**: 5000 

**Related**:
 
- [job property: ms.work.unit.partition](ms.work.unit.partition.md)

## Description

ms.work.unit.parallelism.max defines maximum possible parallel work 
units that can be processed in one job execution.

Gobblin allows multiple work units be executed in one job. The concurrent 
task executor number is controlled by `taskexecutor.threadpool.size`. The thread pool 
size decide the number of work units that will be executed currently. 
By default, the thread pool is 10.

The parameter ms.work.unit.parallelism.max put a limit on the total number of 
work units for a single job execution. In that sense, the thread pool size 
cannot be larger than the total number of work units, because if thread pool 
is bigger than the total number of work units, some threads will be idle. 
Therefore, ms.work.unit.parallelism.max also sets the maximum parallelism for the job.

Typically, for jobs processing large number of work units, we will 
use these two parameters to control job execution. `ms.work.unit.parallelism.max` controls 
how many work units will be processed by the job, and `taskexecutor.threadpool.size` controls 
how many task executor threads will be running concurrently. 
If there are more work units than the number of task executors, a task executor 
will take another work unit once it finished processing its assigned work unit.

For example, a Common Crawl ingestion has about 56,000 files, each about 1 GB. 
Since processing 56K files takes about 2 weeks, we don't want 1 job keep running for 
2 weeks. That for sure will fail. Therefore, we set `ms.work.unit.parallelism.max = 300`, 
that means each job execution will process 300 files. 
Further we don't want to ingest those 300 files all in once, we set `taskexecutor.threadpool.size = 10`, 
that means only 10 files will be ingested concurrently. 
After finished processing the first 10 files, task executors will move to processing 
next 10 files, until all 300 files are processed. Then the job will complete.

When the job is run next time, the first 300 files will be bypassed 
based on their state store records. The next 300 files will be picked up 
and each will generate one work unit, total 300 work units. Again, task executors 
will process those 300 work units in groups of 10 like in the first job execution.

That keeps going until all 56K files are processed

ms.work.unit.parallelism.max is optional. If there are only a few work units 
for a job, it is not necessary to set the limit, and all work units 
will be processed in 1 job execution. In such case, the task executors will 
recursively process work units the same way based on thread pool size.

Therefore, unless there are 100s or more work units for a job, it is not necessary 
to set `ms.work.unit.parallelism.max`, or it can be set to 0 (default), which means no limit.

If ms.work.unit.parallelism.max is set to any value greater than 0, and there are 
more work units than ms.work.unit.parallelism.max, then the Gobblin job need to 
be executed repeatedly until all work units are processed.

## Example	
Total work units is 56,000

`ms.work.unit.parallelism.max = 300`

`taskexecutor.threadpool.size = 10`

Each job execution takes about 1 hour, repeating the job hourly for about 10 days, 
until all work units processed.

[back to summary](summary.md)