# Config Data Persistence

Data persistence includes writer and publisher configuration. Writer writes data into a working directory, and publisher
pushes data to the final storage, such as a data lake. 

- `writer.destination.type: HDFS`
- `writer.dir.permissions=750`
- `writer.file.path.type=namespace_table`
- `writer.fs.uri: hdfs://host:port`
- `writer.dir.permissions=750`
- `writer.include.record.count.in.file.names=true`
- `writer.output.format: AVRO`

[Back to Summary](summary.md#config-data-persistence)