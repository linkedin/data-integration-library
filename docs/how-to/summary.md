# [Config Source and Authentication](source-authentication.md)

Information about the source, such as the protocol and host name, are specified via [ms.source.uri](../parameters/ms.source.uri.md).
The source URI can contain variables. Using variables makes it dynamic. For example, if a job needs to pull many
entities from the same API endpoint, the entity ID be represented with a variable so that we just need one job configuration,
not many repeating job configurations.

# [Config a Status Checking Job](status-check-job.md)

A [status checking job](../concepts/job-type.md#status-checking-job) ensures the data is
ready for consumption on the third party system.

# [Config a Authentication Job](authentication-job.md)

An authentication job is needed mostly in cases where credentials or tokens have to
be refreshed in each flow execution, such as in the case of OAuth2.0 authentication.
