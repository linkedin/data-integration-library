# job.commit.policy

**Tags**: 
[gobblin](categories.md#gobblin-properties)

**Type**: string

**Default value**: full

**Related**:

## Description

`job.commit.policy` specifies how the job state will be committed when some of its tasks failed. Valid values are: 
- full: Commit output data of a job if and only if all of its tasks successfully complete.
- successful: Commit output data of tasks that successfully complete.
- partial: Deprecated, the replacement is "successful"

[back to summary](summary.md#essential-gobblin-core-properties)
