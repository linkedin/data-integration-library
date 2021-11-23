# Variables

A variable provides dynamic input through substitution to parameters. 

## Variable Definition 

Variables are named with only the following letters:
- a-z
- A-Z
- 0-9
- "_" (Underscore)
- "-" (Hyphen)
- "." (dot)
- "$" (dollar sign)

Variables are defined and generated through following means:

### Option 1: [ms.parameters](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.parameters.md)
 
Each parameter defined in `ms.parameters` is also a variable. Parameters defined this
way are called "parameters" because they will be used in final requests to 
integration points (IPs). 

Each parameter is also a variable, but other variables are not used in final requests.
Other variables only participate in value substitution. 

Note this recursive nature of variables and parameters, so you will 
never define a parameter with a substitution variable of itself, that 
will be a loop. 

### Option 2: [ms.watermark](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.watermark.md)

Each `time watermark` defined 2 variables, low and high, but currently 
we support only 1 time watermark. 
So there are 2 time watermark variables. 

Each unit watermark define a variable, 
but currently we support only 1 unit watermark. So there 
is 1 unit watermark variables from properties. The unit watermark 
can have multiple units (values), each of them will be 
distributed into one of the work units, and therefore, 
each work unit has 1 unique value under the same variable name.

### Option 3: [ms.pagination](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.pagination.md)

Each pagination attribute define a variable, these include 
"pagestart", "pagesize", and "pageno". They get values from fields 
defined in ms.pagination, or derived from response data

Example: `ms.pagination={"fields": ["offset", "limit"], "initialvalues": [0, 5000]}`     

### Option 4: [ms.session.key.field](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.session.key.field.md)

A session variable gets value from the field defined in 
`ms.session.key.field`. 

### Option 5: [ms.secondary.input](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.secondary.input.md)

An `activation` secondary input can have multiple records and multiple fields. 
Hence, every `activation` type secondary input can generate one or more variables.
One variable is generate from each of the `fields` selected.

However, `activation` records are distributed to multiple work units, each work 
unit has only 1 row from the activation. So variables generated this way 
are work unit specific, i.e., each work unit has the same variable 
but different value. For example, for Qualaroo, the first job, job1, 
generates a list of surveys, and the list of surveys is used as secondary 
input in the second job, job2. In job2, we select the surveyId 
field from the secondary input, therefore, we have a variable called "surveyId",  
but we have many surveys, thus, we have many work units, 
each work unit has a parameter called "surveyId", and it has a 
value assigned to that work unit.  

An `authentication` secondary input can have 1 and only 1 records, 
but allow multiple fields. That means it can generate multiple variables,
with each variable having only 1 value. 
`authentication` variable will be shared across all work units. 

## Scope of Variables

Variables defined using above methods can be classified into 3 categories:
1. Job level variables
2. Work-unit level static variables
3. Work-unit level dynamic variables

### Job-level Variables

Job-level variables are static; they have the same value across all work units. Variables defined through the following 
methods are job-level: 

- Variables defined in `ms.parameters` that don't use any work-unit level variables
- The range of time watermark defined in [ms.watermark](../parameters/ms.watermark.md)
- Secondary input variables from Authentication category
- Pagination initial values

### Work-unit-level Static Variables

Work-unit level variables can be static or dynamic. The static work unit variables include the following:
- Variables defined in `ms.parameters` that contain reference to work-unit level static variables
- Variables from unit watermark that is defined in [ms.watermark](../parameters/ms.watermark.md)
- Variables from partitioned time watermark that is defined in `ms.watermark` and [ms.work.unit.partition](../parameters/ms.work.unit.partition.md)
- Secondary input variables from Activation category 

### Work-unit-level Dynamic Variables

Work-unit level variables can be static or dynamic. The dynamic work unit variables include the following:
- Variables defined in `ms.parameters` that contain reference to work-unit level dynamic variables
- Session variables
- Pagination current values

## Parameters

Parameters are used to execute requests (Http request, JDBC request, and Sftp request, etc). 
Parameters are also variables, but other variables are just kept internally.  
Other variables are not used to execute requests.

All variables are used for parameter substitution. Therefore, parameters 
can be used in variable substitution.  

Some data source doesn't allow extra parameters in requests. 
For example, SFDC fails if unknown parameters are in the Http request.
Therefore, parameters need to be selective, and only 
parameters that will work with the source should be defined in `ms.parameters`. 

## Usage of Variables

Variables can be used for substitution in parameters (defined through ms.parameters), 
using the syntax of double brackets `{{variableName}}`.
 
If the variable is a parameter that starts with "**tmp**", then once it is used, 
the parameter is removed from the pool.

A parameter can be used for substitution in `ms.extractor.target.file.name`, 
but it will stay in the pool, because the substitution 
happens in Extractor. 

### HTTP Usage

For HTTP protocol, variables are used for substitution in the following properties:

- [ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md)
- [ms.http.request.headers](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.http.request.headers.md)
- [ms.authentication](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.authentication.md)
- [ms.derived.fields](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.derived.fields.md)  
- [ms.extractor.target.file.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extractor.target.file.name.md)

The rest parameters will be appended as URL parameters to `ms.source.uri` 
if it is a GET Http request.

The rest parameters will be added to the Request Entity if it 
is a **POST**, **PUT**, **DELETE** request.

### JDBC Usage

For JDBC protocol, variables are used for substitution in the following properties:

- [ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md)
- [ms.derived.fields](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.derived.fields.md)  
- [ms.jdbc.statement](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.jdbc.statement.md)
- [ms.extractor.target.file.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extractor.target.file.name.md)

### S3 Usage

For S3 protocol, variables are used for substitution in the following properties:
- [ms.source.uri](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md)
- [ms.derived.fields](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.derived.fields.md)  
- [ms.extractor.target.file.name](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.extractor.target.file.name.md)

[Back to Summary](summary.md#variables)