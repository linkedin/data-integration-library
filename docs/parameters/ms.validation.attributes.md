# ms.validation.attributes

**Tags**: 
[conversion](categories.md#conversion-properties)

**Type**: string

**Format**: A JsonObject

**Default value**: blank

**Related**:

## Description

`ms.validation.attributes` defines a threshold to mark a job as successful or failed. 
The threshold can be specified as "success" or "failure" thresholds. The former is 
called a "success" rule, and the later is called a "failure" rule. 

This property is required for **InFlowValidationConverter**, which
is validation converter based on simple count comparison.
 
In either configuration, job will only succeed if the threshold is met. That means, if the
rule is defined as "success", the success rate has to be above the threshold; 
and the failure rate has to be below the threshold if the rule is defined as "failure" type. 

"**success**" rule is used when the data available for validation are successful records. 
"success" rule is a lower bound rule, it works this way:
- Job succeeds when the row count in validation set / row count in base set >= threshold
- Job fails when the row count in validation set / row count in base set < threshold

"**failure**" rule is used when the data available for validation are error records. 
"failure" rule is a upper bound rule, it works this way: 
- Job succeeds when the row count in validation set / row count in base set < threshold
- Job fails when the row count in validation set / row count in base set >= threshold
  
A rule is accepted as a JsonObject with following Keys
- **threshold**: represents the percentage of acceptable failure or required success
- **criteria**: this value can be "fail" or "success"
- **errorColumn**: this value is optional, and it is required in order to filter 
the failure records based on a column, and the records having none-blank 
value in "errorColumn" are considered failed.

### Examples

Failed records cannot be more than 10% of total records:

`ms.validation.attributes={"threshold": "10", "criteria" : "fail"}`

There have to be at least 1 successful record:

`ms.validation.attributes={"threshold": "0", "criteria" : "success"}`

Failed records cannot be more than 30% of total records:

`ms.validation.attributes={"threshold": "30", "criteria" : "fail"}`
       
[back to summary](summary.md#msvalidationattributes)   