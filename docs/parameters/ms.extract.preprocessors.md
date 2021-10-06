# ms.extract.preprocessors

**Tags**: 
[conversion](categories.md#conversion-properties)

**Type**: string

**Default value**: blank

**Related**:
- [job property: ms.extract.preprocessor.parameters](ms.extract.preprocessor.parameters.md)

## Description

`ms.extract.preprocessors` define one or more preprocessor classes that
handles the incoming data before they can be processed by the extractor. 

When input data is compressed or encrypted, the input stream needs to 
be preprocessed before it can be passed to an DIL extractor to parse.  

`ms.extract.preprocessors` is a comma delimited string if there are 
more than 1 preprocessors.

Currently, DIL can consume GZIP'ed and/or GPG encrypted data. 

### Example

The following define preprocessors for GPG encrypted, GZIP compressed, and
compressed and then encrypted data. 

- `ms.extract.preprocessors=com.linkedin.cdi.preprocessor.GpgDecryptProcessor`
- `ms.extract.preprocessors=com.linkedin.cdi.preprocessor.GunzipProcessor`
- `ms.extract.preprocessors=com.linkedin.cdi.preprocessor.GpgProcessor,com.linkedin.cdi.preprocessor.GunzipProcessor`



[back to summary](summary.md#msextractpreprocessors)
