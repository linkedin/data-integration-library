# Json Path

Json structures are nested by nature. JsonObjects can have children JsonObjects or JsonArrays, and JsonArray
can have JsonObjects or JsonArrays. Each field in a JsonObject is called a path segment, and each element index
in a JsonObject is also called a path segment. 

Json Path is a string of segments used to identify a nested element in a Json structure. For JsonObject,
each segment represents a field name of the structure; for JsonArray, each segment represents an index of
a record within the array.

For example, "results.record" represents the "record" field within the "results" field of a JsonObject. 
"results.0.name" represents the "name" field of the first record with the "results" array of a JsonObject.

Json Path is used in the following scenarios:
- identify the actual data element in Json response, see [ms.data.field](../parameters/ms.data.field.md)
- identify the source of a derived field, see [ms.derived.fields](../parameters/ms.derived.fields.md)
- identify the field to be encrypted, see [ms.encryption.fields](../parameters/ms.encryption.fields.md)

[Back to Summary](summary.md#job-pattern) 