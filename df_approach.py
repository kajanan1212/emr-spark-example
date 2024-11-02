from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date
from pyspark.sql.types import StructType, StructField, StringType


spark = SparkSession.builder.appName('a01-big-data-analytics-df-approach').getOrCreate()

s3_input_path_voice_sample = 's3://a01-big-data-analytics/data/voice_sample.csv'
s3_input_path_cell_centers = 's3://a01-big-data-analytics/data/cell_centers.csv'

df1 = spark.read.csv(s3_input_path_voice_sample, header=True, inferSchema=True)
df2 = spark.read.csv(s3_input_path_cell_centers, header=True, inferSchema=True)

df1 = df1.withColumn('CALL_DATE', to_date('CALL_TIME', 'yyyyMMddHHmmss'))

df3 = (
    df2
    .filter(df2['PROVINCE_NAME'] == 'Western')
    .join(df1, ['LOCATION_ID'])
    .groupBy('CALLER_ID').agg(countDistinct('CALL_DATE').alias('NO_OF_DISTINCT_CALL_DATE'))
    .filter(col('NO_OF_DISTINCT_CALL_DATE') == df1.select('CALL_DATE').distinct().count())
    .select('CALLER_ID')
)

s3_output_path = 's3://a01-big-data-analytics/results/'

df3.write.mode('overwrite').csv(s3_output_path + 'df_approach_result_csv', header=True)

schema = StructType([StructField('COUNT', StringType())])
count_df = spark.createDataFrame([(str(df3.count()),)], schema)
count_df.write.mode('overwrite').text(s3_output_path + 'df_approach_result_count')

spark.stop()
