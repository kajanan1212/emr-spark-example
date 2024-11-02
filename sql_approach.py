from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date
from pyspark.sql.types import StructType, StructField, StringType


spark = SparkSession.builder.appName('a01-big-data-analytics-sql-approach').getOrCreate()

s3_input_path_voice_sample = 's3://a01-big-data-analytics/data/voice_sample.csv'
s3_input_path_cell_centers = 's3://a01-big-data-analytics/data/cell_centers.csv'

df1 = spark.read.csv(s3_input_path_voice_sample, header=True, inferSchema=True)
df2 = spark.read.csv(s3_input_path_cell_centers, header=True, inferSchema=True)

df1.createOrReplaceTempView("voice_sample")
df2.createOrReplaceTempView("cell_centers")

df1 = spark.sql('SELECT *, TO_DATE(CALL_TIME, "yyyyMMddHHmmss") AS CALL_DATE FROM voice_sample')

df1.createOrReplaceTempView("voice_sample")

df3 = spark.sql(
    '''
    SELECT CALLER_ID
    FROM (
        SELECT CALLER_ID, COUNT(DISTINCT CALL_DATE) AS NO_OF_DISTINCT_CALL_DATE
        FROM (
            SELECT LOCATION_ID
            FROM cell_centers
            WHERE PROVINCE_NAME = 'Western'
        )
        LEFT JOIN voice_sample
        USING (LOCATION_ID)
        GROUP BY CALLER_ID
    )
    WHERE NO_OF_DISTINCT_CALL_DATE = (SELECT COUNT(DISTINCT CALL_DATE) FROM voice_sample)
    '''
)

s3_output_path = 's3://a01-big-data-analytics/results/'

df3.write.mode('overwrite').csv(s3_output_path + 'sql_approach_result_csv', header=True)

schema = StructType([StructField('COUNT', StringType())])
count_df = spark.createDataFrame([(str(df3.count()),)], schema)
count_df.write.mode('overwrite').text(s3_output_path + 'sql_approach_result_count')

spark.stop()
