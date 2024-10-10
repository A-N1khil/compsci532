from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# Create a spark session with the class name
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the input file
hamlet_df = spark.read.text("hamlet.txt").cache()
# hamlet_df.show()

## Print dimensions of the dataframe
# print((hamlet_df.count(), len(hamlet_df.columns)))

# Split the lines into words (but this will come as a PySpark Column)
split_df = split(hamlet_df.value, ' ')

# Explode the words into rows
explode_df = explode(split_df).alias('split_words')

# Now we are ready to count the words (which basically are the rows)
words_df = hamlet_df.select(explode_df)
word_count = words_df.count()

# Print the word count
print("Word count: ", word_count)

# Stop the spark session
spark.stop()