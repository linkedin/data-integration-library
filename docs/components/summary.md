# Key Component
## Source Classes

Source classes represents the protocol of connections with data systems. In the Gobblin framework, 
a source class actually acts in two roles:

- As the planner when the job starts by generating work units and initiating extractors 
- As an agent in each work unit on behalf of the job when the work unit is picked up a task executor

In DIL the work unit generation function is unanimous across all protocols, hence it is handled by [MultistageSource](https://github.com/linkedin/data-integration-library/blob/master/cdi-core/src/main/java/com/linkedin/cdi/source/MultistageSource.java).
The extractor is initiated with a connection object, and the connection object is tied to the protocols, hence the initiation
is handled by separate sub-classes:

- For HTTP protocol, it is [HttpSource](https://github.com/linkedin/data-integration-library/blob/master/cdi-core/src/main/java/com/linkedin/cdi/source/HttpSource.java)
- For HDFS protocol, it is [HdfsSource](https://github.com/linkedin/data-integration-library/blob/master/cdi-core/src/main/java/com/linkedin/cdi/source/HdfsSource.java)
- For JDBC protocol, it is [JdbcSource](https://github.com/linkedin/data-integration-library/blob/master/cdi-core/src/main/java/com/linkedin/cdi/source/JdbcSource.java)
- For SFTP protocol, it is [SftpSource](https://github.com/linkedin/data-integration-library/blob/master/cdi-core/src/main/java/com/linkedin/cdi/source/SftpSource.java)
- For S3 protocol, it is [S3SourceV2](https://github.com/linkedin/data-integration-library/blob/master/cdi-core/src/main/java/com/linkedin/cdi/source/S3SourceV2.java)

Each subclass holds a set of job keys, so that the extractors can have proper execution context; therefore, the agent
function is handled in sub-classes. 

## [AvroExtractor.md](AvroExtractor.md)
## [CsvExtractor.md](CsvExtractor.md)
## [FileDumpExtractor.md](FileDumpExtractor.md)
## [GunzipProcessor.md](GunzipProcessor.md)
## [InFlowValidationConverter.md](InFlowValidationConverter.md)
## [JsonExtractor.md](JsonExtractor.md)
## [Normalizer-converter.md](normalizer-converter.md)
## [S3SourceV2.md](S3SourceV2.md)
