# ms.data.field

**Tags**:
[source](categories.md#source-properties)

**Type**: string

**Format**: a Json path string separated by "." (dot)

**Default value**: blank

**Related**:

## Description

In a nested response, like JSON or Avro, `ms.data.field` specifies
where the core data (payload) is. 

If data.field is not specified, or is blank, then the whole 
response will be treated as the payload.  

If DIL cannot find the element using the path, as specified by 
`ms.data.field`, it will generate an error.

For nested JsonObjects or Avro Records, the syntax for specifying
the path is concatenating the path segments with ".". 

If at certain level, the element is a JsonArray or Avro Array, this 
function allows pick only **one** element from the array. The syntax of 
picking the element is "A.n". Here element A has an array sub-element,
and we will pick the N(th) element from the array. If the array item is 
an object (JsonObject or Avro Record), the nested path can go on.

### Example

The following picks the core data, or payload, from the `results` element.
- `ms.data.field=results`

If the core data is in the "elements" element of 
the "result" element, it can be specified as:
- `ms.data.field=result.elements`

The following picks `partialFailureError` from the response, and 
then pick `details` from it, then pick the first row in the array,
and then pick the `errors` element.
- `ms.data.field=partialFailureError.details.0.errors`

[back to summary](summary.md#msdatafield)

