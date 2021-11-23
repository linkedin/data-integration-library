# Config Data Persistence

Data persistence includes writer and publisher configuration. Writer writes data into a working directory, and publisher
pushes data to the final storage, such as a data lake. 

To persist data into a HDFS folder in AVRO format, the job need the following optional configuration: 

- `writer.builder.class=gobblin.writer.AvroDataWriterBuilder`, optional writer class, default is Avro data writer
- `writer.file.path.type=namespace_table`, optionally change how the sub-folders should be structured, default
- `data.publisher.replace.final.dir`, optionally truncate the directory if it is a staging area for onetime use in each execution
  
and the following standard configurations:

- `data.publisher.final.dir=/path`
- `writer.destination.type=HDFS`
- `writer.fs.uri=hdfs://host:port`
- `writer.dir.permissions=750`
- `writer.include.record.count.in.file.names=true`
- `writer.output.format=AVRO`

To persist data into ORC format, the following configurations are needed:

TODO

To persist data into partitioned folder structures, the following configurations are needed:

TODO

[Back to Summary](summary.md#config-data-persistence)
