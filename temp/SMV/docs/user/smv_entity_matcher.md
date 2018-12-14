# SMV Entity Matcher

It's a quite common task to "match" two or more datasets on some entities, such as people with
name and address, or companies with doing-business-as names and address. Typically the task
can be broken down to multiple of 2 datasets matching.

Assume we have 2 datasets, DS1 and DS2, typically,
* DS1 has an ID field, and a group of informational fields of the entity
* DS2 has another ID field, and another group of informational fields

It's not necessary that the entities within either DS1 or DS2 have no duplicates. However
We can always make the IDs unique (as primary key) for each dataset.

* Both ID fields in DS1 and DS2 are primary keys of each source

`SmvEntityMatcher` library is a tool set to match 2 datasets. The matching between DS1 and DS2
could be multiple to multiple. This library by itself will not address the dedup problem.

## MultiLevel Matching Framework

The term **MATCHING** here typically means that no perfect deterministic logic can be applied to
**JOIN** DS1 and DS2. To handle the non-deterministic nature, we basically need
to figure out a distance measure between any 2 records from the 2 datasets.

However for different business problems and different use cases, the distance measure could be
different. Also without trying out different possible measures, we may not be able to
predetermine which measures or measure combinations should be better.

To increase the flexibility, the `SmvEntityMatcher` provides a multilevel matching framework.
In more mathematical term, it's a multi dimensional distance measure. For example,
to match 2 records with people's name and address, we could have the following 2 distance measures,
* Levenshtein distance between 2 people's normalized full name, and
* When last name matched, the geo-distance between the 2 addresses

Those different levels (dimensions) could be pretty much independent, or some how correlated. Anyhow
they provide complementary information to the matching problem.

As described above, this framework could be very costly. Assume DS1 has `N` records, and DS2 has
`M` records, and we apply `L` levels matches, the overall complexity is `N * M * L`. Even with
Spark, it's a very expensive task.

To reduce the complexity, we made the following assumptions,
* For each record in each datasets, there are a relatively small set of records in the other dataset it matches (if not zero)
* There are cases a "Full" match can be applied first, and then filtered out all Fully matched records from both sides
* All the matching levels could have a shared necessary condition so that we can split the `N * M` problem to a set of smaller `n * m` problems, where `n << N` and `m << M`

To implement those optimization we introduced,
* `ExactMatchPreFilter` - the "full" match condition for pre filter out the fully matched population
* `GroupCondition` - the "necessary" condition of all level of match logics (except the pre-filtered full match)
* `LevelLogic` - a list of multilevel matching logics   

## SMV Entity Matcher Example
Let's use the following DS1 and DS2 to demonstrate the library.

#### Data Frame 1
id|first_name|last_name|address|city|state|zip|full_name
---|----------|---------|-------|----|-----|---|---------|
1|George|Jetson|100 Skyway Drive|Metropolis|CA|90210|George Jetson
2|Fred|Flintsone|900 Rockaway Road|Pebbleton|CA|90210|Fred Flintstone
3|George|Washington|1600 Pennsylvania Avenue|Washington|DC|20006|George Washington

#### Data Frame 2
_id|_first_name|_last_name|_address|_city|_state|_zip|_full_name
---|----------|---------|-------|----|-----|---|---------
1|Fred|Flintsone|900 Rockaway Road|Pebbleton|CA|90210|Fred Flintstone
2|Alice|Kramden|328 Chauncey Street|Brooklyn|NY|11233|Alice Kramden
3|Georje|Jetson|101 Skyway Drive|Metropolis|CA|90120|Georje Jetson

The data frames should have all the
columns you are planning to use in the matching algorithms.  They must also have one id column
that uniquely identifies each record. Since the matching algorithms need to refer to both
DS1 and DS2's columns, they need to be named differently, so the example DS2 has all the
column names prefixed with `_`

The following is a piece of sample matching code which has
* An `ExactMatchPreFilter` - if someone's full name matched exactly, we consider the 2 records are matched fully, and filtered from any level logics later  
* A `GroupCondition` - a necessary condition of all level matchers is the `soundex` of the 2 first names have to be the same
* An `ExactLogic` - if the 2 records have the same first name, we call it a `First_Name_Match`
* A `FuzzyLogic` - if the 2 records's city names' normalized Levenshtein similarity larger than `0.9`, we call it a `Levenshtein_City` match

```python
resultDF = SmvEntityMatcher("id", "_id",
  ExactMatchPreFilter("Full_Name_Match", col("full_name") == col("_full_name")),
  GroupCondition(soundex(col("first_name")) == soundex(col("_first_name"))),
  [  
    ExactLogic("First_Name_Match", col("first_name") == col("_first_name")),
    FuzzyLogic("Levenshtein_City", lit(True), normlevenshtein(col("city"), col("_city")), 0.9)
  ]
).doMatch(df1, df2, False)
```

We have to specify the ID fields of the 2 datasets, which are the first 2 parameters of `SmvEntityMatcher`s instruction
method.

The `doMatch` method takes a 3rd parameter, which is `keepOriginalCols`. If it's set, all the original columns of
DS1 and DS2 will be passed down to output, if not, the output will be only the matching flags and IDs. Default value
of `keepOriginalCols` is `True`.

#### Output
id|\_id|Full_Name_Match|First_Name_Match|Levenshtein_City|Levenshtein_City_Value|MatchBitmap|
---|---|---|---|---|---|---
2|1|true|null|null|null|100
1|3|false|false|true|1.0|001

Since we didn't keep the original columns, the output data only have matching related flags.
All the 3 matching levels (including the `ExactMatchPreFilter`) have flags respectively.

The `FuzzyLogic` has an addition column which in this case is the normalized Levenshtein distance.
We keep the distance measure itself for potentially future combined matching logics or dedup
logics.

The `MatchBitmap` column summarize the overall matching between the 2 records.

The id pair
`2, 1` has value `100`, which means those 2 records has the `ExactMatchPreFilter` matched.
Since the records, which satisfied `ExactMatchPreFilter`, got filtered, so no further matching
were performed and the `MatchBitmap` can only in the form of `10..0`.  

The id pair `1, 3` has value `001`, which means that only the `Levenshtein_City` logic matched
the 2 records.

## String Distance Measures

The [Stringmetric](https://github.com/rockymadden/stringmetric) library were use to implement
a group of string distance measures as DataFrame UDFs. The following functions were implemented
in `smvfuncs`,

* nGram2
* nGram3
* diceSorensen
* normlevenshtein
* jaroWinkler

All of them normalized from 0 to 1 (0 is no match, 1 is full match).

## Library API Details

### Import Library

```python
from smv.matcher import *
```
To use the [String Distance Measures](#string-distance-measures), users also need to add:  
```python
from smv.functions import *
```

### NoOpPreFilter and NoOpGroupCondition
Both or either of `PreFilter` and `GroupCondition` could be NoOp objects.

When use `NoOpPreFilter` as the `PreFilter` of a `SmvEntityMatcher`, not `full` match is checked.
When use `NoOpGroupCondition`, there are no grouping optimization.

Please avoid using the 2 NoOp objects togethers unless the datasets are both very small.

### GroupCondition Expression Parameter
The expression parameter of `GroupCondition` has to be a `EqualTo` expression. Since
we need to use it as a join condition, and join on `EqualTo` can be optimized by grouping.
Other expression can't help.
```python
GroupCondition(soundex(col("first_name")) == soundex(col("_first_name")))
```

### FuzzyLogic
`FuzzyLogic` takes 4 parameters,
* colName,
* predicate,
* valueExpr,
* threshold

The matching logic is
```
predicate AND (valueExpr > threshold)
```

For example,
* predicate - `col("zip") == col("_zip")`
* valueExpr - `normlevenshtein(col("address"), col("_address"))`
* threshold - `0.6`

The logic is:
```
In the same Zip code AND norm-Levenshtein similarity of the address fields are greater than 0.6
```
