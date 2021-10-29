# 2-step File Download Pattern

In downloading files from cloud (S3, GCS, Azsure) or FTP (SFTP), the preferred
practice is to do it in 2 steps if the number of objects to be downloaded can be 
more than 1. The first step is to list all the files, and save into a staging file;
the second step is to download the files one by one. 

The advantages of using the 2-step method are:

- The files can be downloaded individually through parallel processes.
- The files can be tracked individually, and one file's failure will not impact others.
- The staging file can be useful information for monitoring and troubleshooting.

Here is how to do so.

## Step 1: Retrieve File Names

Step 1 is to configure a job that reads from the source all file names, and 
save the list of file names into a file.  

### Base Configuration

Just like other jobs, the base configurations include source, 
authentication, and others in order to retrieve the needed
status information.

Following are typical settings and explanation:

- `data.publisher.final.dir=<<job_staging_area>>`
- `data.publisher.replace.final.dir=true`
- `writer.file.path.type=namespace_table`

**Explanation**: This job is auxiliary, hence the results are normally not 
persisted into final storage, but you can also make it into
final storage if needed. In above, `data.publisher.replace.final.dir`
and `writer.file.path.type` settings ensure the staging folder
is cleaned up everytime, so that prior files are not accumulated.    

- `extract.table.type=snapshot_only`
- `state.store.enabled=false`
- `extract.is.full=true`

**Explanation**: For the same reason of being auxiliary, we typically don't want to track incremental
values each time the list is pulled.

### Step-1 Configuration

- `ms.extractor.target.file.name=`

**Explanation**:  By setting `ms.extractor.target.file.name` 
to blank, DIL will output the results of the "list" command
to files. The whole logic works as the following:

- retrieve a list of files based on `ms.source.uri` configuration 
- is `ms.extractor.target.file.name` blank?
    - if is blank:
        - List the files and output the results as CSV
    - if is not blank:
        - the number of files == 1
            - dump the file content
        - the number of files > 1
            - dump the first file which matches the pattern

**Note**: Per above logic, it is also sufficient by 
setting `ms.source.files.pattern` to blank.  

**Note**: `ms.source.uri` can have patterns acceptable to the "list"
command, e.g. `ms.source.uri=/home/user/download/*.gpg`. 
 
- `ms.output.schema=[{"columnName":"filePath","isNullable":"true","dataType":{"type":"string"}}]`

**Explanation**: give the output an explict schema because the data retrieved
from the "list" command has not schema. In this case, we will label the
file names as "filePath". 

- `converter.classes=com.linkedin.cdi.converter.CsvToJsonConverterV3,org.apache.gobblin.converter.avro.JsonIntermediateToAvroConverter`

**Explanation**: because the output of the "list" command is CSV, we will
convert it to Json and then to Avro. 

## Step 2: Download File Individually 

Step 2 is to configure a job that reads the file names from step 1, and 
initiate a downloading work unit for each of them.   

### Base Configuration

Just like other jobs, the base configurations include source, 
authentication, and others in order to retrieve the needed
status information.

Following are typical settings and explanation:

- `state.store.enabled=true`

**Explanation**: Enable state store so that files can be tracked
individually.

### Step-2 Configuration

#### Required Configurations

- `ms.secondary.input=[{"path": "<<step 1 job staging area>>", "fields": ["filePath"]}]`

**Explanation**: configure the location to read the list file, and specify the field
that contain file names (paths). The field name "filePath" will  lead to 
a dynamic variable being generated internally. 

- `ms.source.uri={{filePath}}`

**Explanation**: specify that the source is the path name. If
there are 10 files (10 paths), the secondary input will get 10 records, and each
will be assigned to a work unit. Then each work unit will have a path in the
dynamic variable {{filePath}}.  

- `ms.extractor.target.file.name={{filePath}}`

**Explanation**: by setting `ms.extractor.target.file.name`, DIL will
dump the file. 

#### Optional Configurations

- `ms.work.unit.parallelism.max=100`

**Explanation**: optionally can limit the number of files to download when 
there are large number of files. In such case, the step 2 job can be repeated
to download all files in batches of 100.

## Adding a Status Check Step

The 2-step process can be modified with a status check upfront, 
the status check job can ensure files are ready before starting 
the downloading process. 

See [how-to: status check](../how-to/status-check-job.md)

[Back to Pattern Summary](summary.md)