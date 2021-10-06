# ms.abstinent.period.days

**Tags**: 
[watermark & work unit](categories.md#watermark-work-unit-properties)

**Type**: integer

**Default value**: 0

## Related 

- [key concept: watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/watermark.md)
- [key concept: work-unit](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/work-unit.md)
- [Property: ms.grace.period.days](ms.grace.period.days.md)
- [How to: abstinent period](https://github.com/linkedin/data-integration-library/blob/master/docs/how-to/abstinent-period.md)
- [How to: grace period](https://github.com/linkedin/data-integration-library/blob/master/docs/how-to/grace-period.md)

## Description

Abstinent Period is designed to avoid re-extracting a dataset repeatedly. This is particularly useful
for situations like downloading files in large quantity.</p>

Abstinent Period works the opposite way of Grace Period. In an incremental ETL execution, 
Abstinent Period is added to a work unit's last execution time for form a new low watermark for 
that work unit, while Grace Period is subtracted from the work unit's last execution time.

Assuming we will control all data extraction through a time range, including file downloads, and 
assuming files were all uploaded to source on 6/30, and further assuming we can only download 100 files
per day, and there are 1000 files, therefore, the plan should be downloading 100 files per day, and run the job for 
10 consecutive days. 

Assuming we start downloading on 7/1, files downloaded on 7/1 will be downloaded again on 7/2 because
their cut off time is 7/1, which is the last actual high watermark, and which is before the new extraction time (7/2).
See how cut off time is calculated [here](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/watermark.md).

An abstinent period 30 is thus added to the last actual high watermark, allowing us move the cutoff time forward.
Therefore, if there is an abstinent period of 30 days, the downloaded files will not be downloaded
again in 30 days. 

Abstinent period can be set to a large number so that the same file will never be downloaded again.

[back to summary](summary.md#msabstinentperioddays)
