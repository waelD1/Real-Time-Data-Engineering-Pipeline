from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, when, udf
from config.config import config
from time import sleep
from openai import OpenAI

import openai

def sentiment_analysis(comment) -> str:
    if comment:
        client = openai.OpenAI(
            api_key=config['openai']['api_key']
        )

        prompt = f"""
            Act as an expert in Sentiment Analysis. Your goal is to classify a comment into one of the three
            following category: "POSITIVE", "NEGATIVE" and "NEUTRAL". Respond with only one word, nothing else added.
            Choose the word according to the sentiment analysis that you made.
            Here is the comment : 
            {comment}
        """

        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content.strip()
    return "There is no comment"


def strart_streaming(spark):
    topic = "customers_reviews"
    while True:
        try : 
            stream_dataframe = (
                                spark.readStream
                                .format("socket")
                                .option("host", "0.0.0.0")
                                .option("port", "9999")
                                .load()
                                )
            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            stream_dataframe_new = stream_dataframe.select(from_json(col('value'), schema).alias('data')).select(("data.*"))

            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            # Link openAI stream to spark stream
            stream_dataframe_sentiment = stream_dataframe_new.withColumn('feedback',
                                                                        when(col("text").isNotNull(), sentiment_analysis_udf(col('text')))
                                                                        .otherwise(None)
                                                                        )

            kafka_df = stream_dataframe_sentiment.selectExpr("CAST(review_id AS STRING) AS key", 
                                                    "to_json(struct(*)) AS value")
            query = (kafka_df.writeStream
                    .format('kafka')
                    .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers']) 
                    .option("kafka.security.protocol", config['kafka']['security.protocol'])
                    .option("kafka.sasl.mechanism", config['kafka']['sasl.mechanisms'])
                    .option('kafka.sasl.jaas.config', 
                            "org.apache.kafka.common.security.plain.PlainLoginModule required username='{username}' "
                            "password='{password}';".format(
                                username = config['kafka']['sasl.username'],
                                password = config['kafka']['sasl.password']
                            ))
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .option('topic', topic)
                    .start()
                    .awaitTermination()
                    )
            
        
        except Exception as e:
            print(f"Exception raised : {e}. Retrying in 10 seconds")
            sleep(10)

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    strart_streaming(spark_conn)