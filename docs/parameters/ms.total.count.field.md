# ms.total.count.field

**Tags**:
[pagination](categories.md#pagination-properties),
[source](categories.md#source-properties)

**Type**: string

**Format**: A [JsonPath](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/json-path.md) separated by ".", like "level1.level2"

**Default value**: blank

## Related 

- [key concept: pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md)

Total Count field directs DIL how to retrieve the expected total row counts. This is important when there are large
volume of data and [pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/concepts/pagination.md) 
is used to retrieve data page by page. In cases of pagination, the expected total row count is one way to indicate
the end of pagination when the actually retrieved total number of rows matches or surpasses the expected total rows.

Total count field is normally the element name representing the expected total count. It is, therefore, mostly in 
simple string format.

If the response is in Json format, the total count field can be in a nested element, and the configuration will 
be a JsonPath to indicate how to find the nested element.  

Example 1: if the response from the source is like this: `{..."total_records" : 48201...}`, then the configuration can be
`ms.total.count.field = total_records`.

Example 2: if the response from the source is like this `{..."records": {..."totalRecords": 9999...}...}`, then
the configuration can be `ms.total.count.field = records.totalRecords`. 

[back to summary](summary.md#mstotalcountfield)




