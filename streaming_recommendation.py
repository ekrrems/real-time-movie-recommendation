from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.stem.porter import PorterStemmer
import pandas as pd

# Set up Spark configuration and create a SparkSession
conf = SparkConf().setAppName("MovieRecommendations")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Set up the StreamingContext with a batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 1)

# Create a KafkaConsumer instance
consumer = KafkaConsumer('movie_recommend_topic', bootstrap_servers=9092,
                         group_id='recommendation_group', auto_offset_reset='latest')

recom_df = pd.read_csv("recom_df.csv")

cv = CountVectorizer(max_features=5000, stop_words="english")
vectors = cv.fit_transform(recom_df["tags"]).toarray()

ps = PorterStemmer()

def stem(text):
    y = []
    for i in text.split():
        y.append(ps.stem(i))

    return " ".join(y)

recom_df["tags"] = recom_df["tags"].apply(stem)

similarity = cosine_similarity(vectors)


def recommend(movie):
    movie_index = recom_df[recom_df["title"] == movie].index[0]
    distances = similarity[movie_index]
    movies_list = sorted(list(enumerate(distances)), reverse=True, key=lambda x:x[1])[1:6]
    recommendations = []
    for i in movies_list:
        recommendations.append(recom_df.iloc[i[0]].title)

    return recommendations

# Create a DStream from the KafkaConsumer's stream of messages
kafka_stream = ssc.queueStream([consumer])

# Process the Kafka stream
def process_stream(rdd):
    if not rdd.isEmpty():
        # Process the incoming data as needed
        # Replace the following line with your desired processing logic
        rdd.foreach(lambda x: print(x))

# Process each batch of data
kafka_stream.foreachRDD(process_stream)

# Start the Spark Streaming context
ssc.start()

# Wait for the termination signal
try:
    ssc.awaitTermination()
except KeyboardInterrupt:
    print("Stopping the Spark Streaming application...")
    ssc.stop()
