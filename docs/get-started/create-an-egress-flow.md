# Egress Flow
 
An egress job is a job that sends data from the platform to an external system. This is useful when you want to send data to a system that is not supported by the platform. For example, you can send data to an external service for instance Amazon S3.

In terms of how DIL functions there is no difference between an egress job and an ingress job. The only difference is that the data is sent to an external system instead of being ingested into the platform. So the parameters and concepts remain similar to ingress jobs.

So all the parameters that are used in an ingress job can be used in an egress job. The only difference is that the `ms.source.uri` parameter is used to specify the destination of the data.

Since we are uploading data specifically for S3 egress we need to specifiy `"format":"binary"` in the `ms.secondary.input` parameter. Also since the data is being uploaded in binary format no converter and extractors will be applied to it, which generally gets applied for ingress flows.

Example: 
> 'ms.secondary.input': '[{"path": "${job.dir}/${preceding.table.name}", "fields":["pathName"], "category": "activation"}, {"path": "{{pathName}}", "category": "payload", "format":"binary"}]'

