# SMV  Entity Matcher

Part of the process of creating a master file during MDM from different data sources is mapping related records.  This can be accomplished with one or more algorithms, where each algorithm could map one record from one data source to multiple records in another.  A single relationship record is an output record containing an id from each of the two input data sources.  Thus a one to many match will result in multiple relationship records.  Each matching algorithm can also provide one or more metrics describing the likelyhood of a match.  Since multiple matching alogrithms can be applied to each relationship record, in adition to ids, each relationship record will have several results from the matching algorithms.  These can be used to determine which relationship records should go into the master table.

## Sample Input
These are the data frames we will use in the examples below

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


## Simplest SMV  Entity Matcher

The SMV  Entity Matcher takes two data frames as parameters.  The data frames should have all the columns you are planning to use in the matching algorithms.  They must also have one id column that uniquely identifies each record.   The name of this column must be "id"

```scala
/*** Input data frames ***/
val df1 = ...
val df2 = ...

/*** Resulting MDM data frame ***/
val resultDF = SmvEntityMatcher(
  null,
  null,
  List(
    ExactLevelMatcher("First_Name_Match", $"first_name" === $"_first_name"),
  )
).doMatch(df1, df2)
```

This example may be unrealistically simplistic, but it's a good place to start to learn how SmvEntityMatcher works.  

We first create the sample input data frames described above.  SmvEntityMatcher will match the records between these.  The output will be resultDF, which will have relationship records for each match with metrics describing the likelyhood of the match.  See output below.

The first two parameters to SmvEntityMatcher are optional.  We'll describe what they are later.  For now we'll just pass nulls for them.  The third parameter is a list of level matchers.  In this example we are using only one - the ExactLevelMatcher.   

Let's break down the name of this class.  It's a matcher because it has an expression to match records from one data frame to another.  The expression in this case compares first name from one to first name in the other.  If they are the same, it's a match.  It's an exact matcher, because the output is a boolean.  As opposed to other matchers, described below, where the output could be a fuzzy number.  The output relationship record will have ids from both data frames and a boolean that will be true if the first names are the same and false if not.  The word "level" is used because you may have different levels of expressions, where level 1 is more important than level 2.  The API takes a List of matchers.  Each of your matchers can describe an expression for its own level.   

Also note that sample data df2 has a column called "first_name", whereas in the expression we used "$_first_name".  To disambiguate column names, all columns from the second datasource need to be prepended with an underscore.

Note that all record relationships below are true.  If every matching expression returns false, then we can assume that these records don't match, and we remove these from the output.  In this simple exmaple we have only one matcher, so every expression result will be true.

### Output
id|\_id|First_Name_Match
---|---|---
2|1|true


## Introducing ExactMatchFilter and FuzzyLevelMatcher
```scala
val resultDF = SmvEntityMatcher(
  ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name"),
  null,
  List(
    ExactLevelMatcher("First_Name_Match", $"first_name" === $"_first_name"),
    FuzzyLevelMatcher("Levenshtein_City", null, StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f)
  )
).doMatch(df1, df2)
```

As you add more level matchers, the number of records that match will increase as well as expression results for each record relationship.  The FuzzyLevelMatcher takes in an exact match parameter, similar to the one for the ExactLevelMatcher.  This is optional, so it's null in the example above.  It also takes a parameter that evaluates to a fuzzy number between 0 and 1.   The result of this expression will be returned as one of two outputs for the FuzzyLevelMatcher.  The other output is a boolean that's determined using this expression:

**(** evaluate \<exact catalyst expression\> **)** **&&** **(** **(** evaluate \<normalized fuzzy catalyst expression\> **)** **\>** \<threshold\> **)**

Note: when using stringmetric fucntions, such as the one in previous example "levenshtein", be sure to normalize the case of the two columns first.

Once you start adding level matchers, you may find an ExactLevelMatcher expression that's more important than the rest.  So much so, that if the records match on it, you don't need to perform any other tests on the resulting record relationship.  In that case you can place this expression in an ExactMatchFilter and provide this as the first parameter to SmvEntityMatcher.  In the example above this expression matches full names of the input data frames.   The output can be seen as having two parts.  The first part will be for record relationships that we created be a matched ExactMatchFilter.  The output column for the filter will be true and the rest of the columns will be null, since they were not evaluated.   All remaining records from the input data frames will be matched as before, by all the list of level matchers.   Here the output column for the filter will always be false.

### Output
id|\_id|Full_Name_Match|First_Name_Match|Levenshtein_City|Levenshtein_City_Value|
---|---|---|---|---|---
2|1|true|null|null|null
1|3|false|false|true|1.0

## Introducing CommonLevelMatcherExpression
```scala
val resultDF = SmvEntityMatcher(
  ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name"),
  CommonLevelMatcherExpression(soundex($"first_name") === soundex($"_first_name"))),
  List(
    ExactLevelMatcher("First_Name_Match", $"first_name" === $"_first_name"),
    FuzzyLevelMatcher("Levenshtein_City", null, StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f)
  )
).doMatch(df1, df2)
```

The sample data frames used here are very small.  Real data frames are much larger and matching every record between them may take too long and in many cases not necessary. In most of the cases, there is always some "common denominator" as the `CommonLevelMatcher`. For example, the `soundex` of first name could be the common requirement of all the matchers. In that case you are able to factor out a common expression from all the expressions you are evaluating. In above example, after the exact match filter, we assume all other match levels have to have `soundex` of first name match. By specifying the `CommonLevelMatcherExpression`, internally the algorithm only need to look at records where the soundex are the same.  Meaning that you can reduce the input data frames to only have such records.  Using CommonLevelMatcherExpression will not change the output, but it can greatly speed up the processing time.

### Output
id|\_id|Full_Name_Match|First_Name_Match|Levenshtein_City|Levenshtein_City_Value|
---|---|---|---|---|---
2|1|true|null|null|null
1|3|false|false|true|1.0
