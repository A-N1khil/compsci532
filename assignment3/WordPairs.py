from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, array, lit
from pyspark.sql.functions import desc, expr
from itertools import combinations

# Initialize Spark session
spark = SparkSession.builder.appName("WordPairs").getOrCreate()

# File path
logFile = "hamlet.txt"  # Replace with the actual path to hamlet.txt

# Read the file into a DataFrame
logData = spark.read.text(logFile)

# Split each line into words
lines_words = logData.select(split(logData.value, ' ').alias('words'))

# Function to generate pairs of words
def word_pairs(words):
    word_list = [word for word in words if word != '']
    return list(combinations(word_list, 2))

# Registering UDF to create word pairs
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf

pair_udf = udf(lambda words: word_pairs(words), ArrayType(ArrayType(StringType())))

# Apply UDF to create pairs of words
word_pairs_df = lines_words.select(explode(pair_udf(col('words'))).alias('pair'))

# Filter out any empty pairs
filtered_pairs = word_pairs_df.filter(expr("size(pair) > 0"))

# Group by word pairs and count occurrences
pair_count = filtered_pairs.groupBy('pair').count()

# Sort the word pairs by frequency in descending order and get the top 20
top_pairs = pair_count.orderBy(desc('count')).limit(20)

# Show the top 20 most frequent word pairs with their counts
top_pairs.show(truncate=False)

# Stop the Spark session
spark.stop()