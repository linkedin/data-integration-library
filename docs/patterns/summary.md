# FLow Design Patterns

## [Asynchronous Ingestion Pattern](asynchronous-ingestion-pattern.md)

## [2-step File Download Pattern](two-step-file-download-pattern.md)

In downloading files from cloud (S3, GCS, Azsure) or FTP (SFTP), the preferred
practice is to do it in 2 steps if the number of objects to be downloaded can be 
more than 1. The first step is to list all the files, and save into a staging file;
the second step is to download the files one by one. 

## [Egression with Validation Pattern](egression-validation-pattern.md)
