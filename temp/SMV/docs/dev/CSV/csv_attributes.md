# Problem
When a user wants to process a CSV file, the onus is currently on them to provide the appropriate CSV Attributes when they make the call.  The CSV attributes should be a property of the file and not of the call.

# Solution
Instead of supplying the CSV attributes in the input/read interface, the CSV attributes must be specified in the schema file.  A set of key/value pairs will be defined that allow the user to define the CSV attributes in the schema file.

<table>
<tr>
<th>Key</th>
<th>Default</th>
<th>Description</th>
</tr>
<tr>
<td>has-header</td>
<td>true</td>
<td>Determine if CSV file has header.  Can only contain true/false</td>
</tr>
<tr>
<td>delimiter</td>
<td>,</td>
<td>CSV field delimiter/separator. For tab separated files, specify \t as the separator</td>
</tr>
<tr>
<td>quote-char</td>
<td>"</td>
<td>character used to quote fields (only used if field contains characters that would confuse the parser)</td>
</tr>
</table>

The above only applies to input methods (reading CSV files).  When we write the CSV file, the user **MUST** supply the CSV attribute parameter (explicitly).  The CSV writing should then convert the CSV attributes into Schema properties and persist it along with the data.  Therefore, subsequent "reads" of the file will use the CSV attributes in the Schema file!

# Example
```
@has-header = true
@delimiter = |
id: String;
age: Integer;
...
```

# Design
To maintain backward compatibility and create an easy migration path, we will not get rid of `CsvAttributes` all together.
Instead, we make the `CsvAttributes` an optional parameter of creating a CSV file.  And if the attributes are not specified explicitly, we will extract the attributes from the schema file.
However, all documentation and examples will only reference the specification of attributes in the schema file.

Effectively, all we really need to do is:
* Make `csvAttributes` parameter of `SmvCsvFile` optional (need to use null instead of None to maintain backward compatibility).
* add a `extractCsvAttributes` to `SmvSchema` class (this is where the magic happens)
