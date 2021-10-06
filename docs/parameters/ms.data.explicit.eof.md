# ms.data.explicit.eof

**Tags**: 
[conversion](categories.md#conversion-properties)

**Type**: boolean

**Format**: true/false

**Default value**: false

**Related**:

## Description

`ms.data.explicit.eof` specifies whether an explicit EOF record should 
be sent to converter after processing all records. 

If enabled, DIL will send a record `{"EOF":"EOF"}` to converter signaling 
no more records. 

By default, Gobblin use a null record to signaling the end of processing,  
but that signal is gobbled by the execution context, and not passed to 
converter at all. Therefore, converter doesn't get the chance to wrap up
things on hand. 

Knowing the end of processing is important for Normalizer Converter, so 
that it can flush out the last batch of records. 

### Example


[back to summary](summary.md#msdataexpliciteof)

