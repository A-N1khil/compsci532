from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, udf, size
import assignment3.data_cleaner as dc
import importlib
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations_with_replacement

# Reload the module
importlib.reload(dc)

# Create a spark session and load text file
spark = SparkSession.builder.appName("word_pairs").getOrCreate()
hamlet_df = spark.read.text("hamlet.txt").cache()

"""
Clean the dataframe but DO NOT split and explode the words

IMPORTANT:
The reason we are not splitting and exploding here is because we have to save the structure of the sentence. 
The question asks us to find the number of count. A pair appears in the same sentence together. 
Thus we have to keep the structure of the sentence until we find all the pairs.
If we were to split and explode the words, we would lose the structure of the sentence
"""
hamlet_df = dc.clean_dataset(hamlet_df, should_split_explode=False)

# Get the list of words and convert them to pairs
def create_pairs(sentence):
    # set will remove duplicates
    all_words = list(set([word for word in sentence.split(' ') if word != '']))

    # Create pairs using itertools
    pairs = list(combinations_with_replacement(all_words, 2))

    # Add pairs to the dataframe as constant string literals.
    return [f"({pair[0]}, {pair[1]})" for pair in pairs]

"""
IMPLEMENTATION NOTE:
The idea here is to take every sentence and split them into words. Once the empty words are removed from the sentence, we can then go on to make all of them in pairs.
The itertools.combinations_with_replacement() method will return all possible pairs of the words in the sentence.
The pairs would then be converted into a constant string. In this way, every time we are done with the sentence we would have all those pairs that have been in that sentence.
Thus giving us a new data frame, which has all the pairs that have ever occurred in the same sentence.
"""
create_pairs_udf = udf(create_pairs, ArrayType(StringType()))
pairs_df = hamlet_df.select(create_pairs_udf(hamlet_df.word).alias('pairs'))

# Filter out null values and empty lists
pairs_df_cleaned = pairs_df.filter(pairs_df.pairs.isNotNull() & (size(pairs_df.pairs) > 0))
all_pairs_df = pairs_df_cleaned.select(explode(pairs_df_cleaned.pairs).alias('pair'))

"""
IMPLEMENTATION NOTE:
Now that we have all the pairs exploded into rose, and all the pairs are constant string, literals of the format we want.
However, we still have to group the same pair as they would have been added to the data frame. Every time a new sentence was passed through the UDF.
Thus we have to group the pairs and count the occurrences of each pair.
"""
# Group the pairs, count the occurrences and show the top 20
all_pairs_df.groupBy('pair').count().orderBy('count', ascending=False).limit(20).show()

# Stop the spark session
spark.stop()