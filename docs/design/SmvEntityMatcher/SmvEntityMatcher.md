## Description

This API allows you to create an MDM out of two input data frames.  The MDM that this API produces will start with an id column from the first data frame and an id column from the second one.  So the first two columns of the MDM output represent a relationship or a mapping between the records of the two input data frames.  Each relationship will have booleans and numbers associated with it, which are caclulated from the catalyst expressions entered by the user.  The results of these will be booleans and floats. The booleans will be the result of a match test on each mapped set of two records.  If the match is a fuzzy  match, or it contains a fuzzy match, the results of this are normalized to a float between 0 and 1.  This number is saved in the MDM under a column name associated  with the boolean result of the match.  The column name of the number just adds "_Value" to the column name of the associated boolean.  For example if the test is  "Levenshtein_City", the boolean match result will be in "Levenshtein_City" and the normalized float value will be in "Levenshtein_City_Value"
  
The user can use these float and boolean results to decide the strength of the relationship of each mapping.

 The two input data frames will likely have the same schema and thus the same column names.  To avoid ambiguity, the first data frame's columns are left as is and the second one's are prepended with an underscore.  Thus df1("last_name") stays as is and df2("last_name") will be referenced as $"_last_name" in the API.

The main class is SmvEntityMatcher.  It's constructed with three parameters: ExactMatchFilter, CommonLevelMatcher, and a List of LevelMatches.  Of these only the LevelMatches are required.  See API Description below for a full explanation of how to configure and use these classes.  Once you construct an SmvEntityMatcher, call the doMatch method on it with the two input data frames.  It will return the MDM.  Only records where at least one of the matches succeeded will be kept in the MDM.



## Client code

```scala
/*** Input data frames ***/
val df1, df2 

/*** Resulting MDM ***/
val resultDF = SmvEntityMatcher(
  ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name"),
  CommonLevelMatcherExpression(StringMetricUDFs.soundexMatch($"first_name", $"_first_name")),
  List(
    ExactLevelMatcher("First_Name_Match", $"first_name" === $"_first_name"),
    FuzzyLevelMatcher("Levenshtein_City", null, StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f)
  )
).doMatch(df1, df2)
```

## Sample Input

### Data Frame 1
id|first_name|last_name|address|city|state|zip|full_name
---|----------|---------|-------|----|-----|---|---------|
1|George|Jetson|100 Skyway Drive|Metropolis|CA|90210|George Jetson
2|Fred|Flintsone|900 Rockaway Road|Pebbleton|CA|90210|Fred Flintstone
3|George|Washington|1600 Pennsylvania Avenue|Washington|DC|20006|George Washington


### Data Frame 2
id|first_name|last_name|address|city|state|zip|full_name
---|----------|---------|-------|----|-----|---|---------
1|Fred|Flintsone|900 Rockaway Road|Pebbleton|CA|90210|Fred Flintstone
2|Alice|Kramden|328 Chauncey Street|Brooklyn|NY|11233|Alice Kramden
3|Georje|Jetson|101 Skyway Drive|Metropolis|CA|90120|Georje Jetson


## Sample Output
Based on the sample input and client code above

id|\_id|Full_Name_Match|First_Name_Match|Levenshtein_City|Levenshtein_City_Value|
---|---|---|---|---|---
2|1|true|null|null|null
1|3|false|false|true|1.0

The first record matches for the ExactMatchFilter.  This always results in its column being set to true and all the other ones being set to null.  It also removes the matched records from the set that will be further processed. The remaining records are matched by joining them on the soundex of the first names from each data frame.  This results in two matches: a match of id 1 from data frame 1 and id 3 from data frame 2, and a match of id 3 from both data frames.  The second match is excluded from the resulting data frame because all boolean matches evaluate to false.   In the other match, the Full_Name_Match field will always be false for any LevelMatch, since only the records that did not match the ExactMatchFilter are being joined.  First_Name_Match evaluates to false because the first names are not the same and Levenshtein_City evaluates to true because the city is the same, making it's levenstein distance be greater than the .9 threshold.


## Decorating Input
If your input data frames don't have all the columns needed by your level matches, we suggest that you write a decorator to create the needed derived columns. For example the Sample Input data frames could have had just the id, name and address fields.  A decorator would then add the full_name field that is derived from the existing fields.

## API Description

###ExactMatchFilter 
This is an optional parameter.  The ExactMatchFilter is a kind of preprocessor that allows you to make only one match on a combination of records from the input data frames.  Any matched ids will then be returned in the MDM with the corresponding MDM column set to true and the rest of the columns set to null.  The matched records will be excluded from the remainder of Levelmatches.

This is useful if you have a very determinitive test that identifies matching records by itself and you can use it to reduce the total number of records that need to be 
matched for the remaining Levels.

```scala
ExactMatchFilter(<column name>, <catalyst expression>) 
// column name - name of the column where the results of the match are stored
// catalyst expression to find the combination of records that match it

//Example:
ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name")
```

###CommonLevelMatcher
This is an optional parameter.  If your Level Matchers have a common expression, you can optimize your tests by applying this expression first in this parameter.  If you set this to null, then Level Matchers will be applied to all combinations of records in whatever was not matched by the Exact Match Filter.
 
###CommonLevelMatcherNone
If you set the Common Level Matcher parameter to null, then internally it will be replaced with CommonLevelMatcherNone.  This will combine all records remaining in the input data frames after the ExactMatchFilter has been applied.  
 
###CommonLevelMatcherExpression
If you have a common level expression, you can specify it here.  A seperate boolean column with the result will not be created in the MDM but the remaining LevelMatchers will perform faster.

```scala
CommonLevelMatcherExpression(<catalyst expression>) // catalyst expression on which to join the data frames
CommonLevelMatcherNone

//Example:
CommonLevelMatcherExpression(StringMetricUDFs.soundexMatch($"first_name", $"_first_name"))
CommonLevelMatcherNone
```

###LevelMatcher
There are two kinds of LevelMatches: ExactLevelMatcher and FuzzyLevelMatcher. The first one should be used if the expression for the level is an exact match expression.  If the expression is a fuzzy match or part of the expression is a fuzzy match, then you should use the FuzzyLevelMatcher.

###ExactLevelMatcher
ExactLevelMatcher takes a column name and a catalyst expression.  The expression operates on the joined data frame and returns a boolean.  The value is returned in the MDM under the column specified by the input column name.
```scala
ExactLevelMatcher(<column name>, <catalyst expression>)
// column name - name of the column where the results of the match are stored
// catalyst expression to find the combination of records that match it

//Example:
ExactLevelMatcher("First_Name_Match", $"first_name" === $"_first_name")
```

###FuzzyLevelMatcher
This creates two columns in the MDM.  The first one is a boolean, returned in a column named after the column name parameter.  The second one is a float that adds "_Value" to the column name parameter: "\<column name\>_Value".  The float is the evaluation of the fuzzy expression

**Parameters:**
**column name** - <column name> for the boolean result
**exact catalyst expression** - Optional. The exact part of the expression.  If there is no exact part, then this should be set to null.  This expression should evaluate to a boolean.
**normalized fuzzy catalyst expression** - the fuzzy catalyst part of the expression.  The returned value should be normalized to return a float between 0 and 1, where the higher number represents a better match.
**threshold** - a number between 0 and 1.  A fuzzy match value greater than that means match, and lower means no match.

#####How the boolean output value is determined
**(** evaluate \<exact catalyst expression\> **)** **&&** **(** **(** evaluate \<normalized fuzzy catalyst expression\> **)** **\>** \<threshold\> **)**

```scala
FuzzyLevelMatcher(<column name>, <exact catalyst expression>, <normalized fuzzy catalyst expression>, <threshold>)

//Example:
FuzzyLevelMatcher("Levenshtein_City", null, StringMetricUDFs.levenshtein($"city",$"_city")), .9)
```


###StringMetricUDFs
Convinience wrappers around various open source similarity and other sound metric algorithms that you can use in your catalyst expressions.  See https://github.com/rockymadden/stringmetric
* soundexMatch(String,String) - Soundex Metric comparison.  Returns boolean.
* levenshtein(String,String) - Levenshtein Metric comparison.  Returns a float between 0 and 1.  The higher the number the stronger the match.

###MDM
The resulting data frame will always have an "id" and "_id" columns reperesenting the mapped ids of the input data frames.  It will also have boolean columns with names taken from the ExactMatchFilter and from the LevelMatches.  And it will have float columns for the results of the fuzzy match from FuzzyLevelMatches.  Only records where at least one of the matches have a true value will be kept in the MDM.

## Constraints
### Valid Input
* Input data frames must have an "id" column that uniquely identifies each record
* Input data frames must not have any column names that end with "_Value"
* Column names can't start with an underscore
* Any parameters not described as optional above are required
* The catalyst expressions must be valid for the kind of data they operate on

### Output Values
* The id fields in the MDM will contain the same data and will be of the same type as the corresponding fields in the input data frames
* The Level columns can either have true, false, or null value for the boolean columns and a number between 0 and 1 or null for the float columns.  For the data returned by the ExactMatchFilter, its column will always be true and all other fields will be null. 


