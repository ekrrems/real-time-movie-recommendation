from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StringType, ArrayType
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.stem.porter import PorterStemmer
import ast
import pandas as pd

spark = SparkSession.builder.appName("Real-Time Movie Recommendation").getOrCreate()

credits_data_file_path = r"C:\Users\EkremSerdar\OneDrive\Masaüstü\Data_Engineering\Pipeline_Projects\movie_recommendation_spark\credits.csv"
movies_data_file_path = r"C:\Users\EkremSerdar\OneDrive\Masaüstü\Data_Engineering\Pipeline_Projects\movie_recommendation_spark\movies.csv"

credits_data = pd.read_csv(credits_data_file_path)
movies_data = pd.read_csv(movies_data_file_path)

merged_df = movies_data.merge(credits_data, on="title")

selected_cols = ["movie_id", "title", "overview", "genres", "keywords", "cast", "crew"]
merged_df = merged_df[selected_cols]

def convert(x):
    L = []
    dictionaries = ast.literal_eval(x)
    for d in dictionaries:
        # Extract the "name" values from the dictionaries
        L.append(d.get("name", ""))
    return L
    

# print(merged_df.genres.head())
merged_df.genres = merged_df.genres.apply(convert)
merged_df.keywords = merged_df.keywords.apply(convert)
merged_df.dropna(inplace=True)

merged_df = spark.createDataFrame(merged_df)


def convert_cast(x):
    L = []
    counter = 0
    for i in ast.literal_eval(x):
        if counter != 3:
            L.append(i["name"])
            counter += 1
        else:
            break
    return L


def fetch_director(x):
    L = []
    for i in ast.literal_eval(x):
        if i["job"] == "Director":
            L.append(i["name"])

    return L

replace_spaces = lambda x: [i.replace(" ", "") for i in x]
join_lambda = lambda x: " ".join(x)
lowercase_lambda = lambda x: x.lower()

convert_cast_udf = func.udf(convert_cast, ArrayType(StringType()))
fetch_driector_udf = func.udf(fetch_director, ArrayType(StringType())) 
replace_spaces_udf = func.udf(replace_spaces, ArrayType(StringType()))
join_udf = func.udf(join_lambda, StringType())
lowercase_udf = func.udf(lowercase_lambda, StringType())


merged_df = merged_df.withColumn("cast", convert_cast_udf(merged_df["cast"]))
merged_df = merged_df.withColumn("cast", replace_spaces_udf(merged_df["cast"]))
merged_df = merged_df.withColumn("crew", fetch_driector_udf(merged_df["crew"]))
merged_df = merged_df.withColumn("crew", replace_spaces_udf(merged_df["crew"]))
merged_df = merged_df.withColumn("overview", func.split(merged_df["overview"], " "))
merged_df = merged_df.withColumn("overview", replace_spaces_udf(merged_df["overview"]))
merged_df = merged_df.withColumn("genres", replace_spaces_udf(merged_df["genres"]))

merged_df = merged_df.withColumn("tags", func.concat(func.col("overview"), func.col("genres"), func.col("keywords"), func.col("cast"), func.col("crew")))


recom_df = merged_df.select(["movie_id", "title", "tags"]) 
recom_df = recom_df.withColumn("tags", join_udf(recom_df["tags"]))
recom_df = recom_df.withColumn("tags", lowercase_udf(recom_df["tags"]))

#recom_df.toPandas().to_csv("recom_df.csv")


# recom_df = recom_df.toPandas()
#cv = CountVectorizer(max_features=5000, stop_words="english")
#print(cv.fit_transform(recom_df["tags"]).toarray().shape)

# vectors = cv.fit_transform(recom_df["tags"]).toarray()
# print(vectors[0])

# ps = PorterStemmer()
# def stem(text):
#     y = []
#     for i in text.split():
#         y.append(ps.stem(i))

#     return " ".join(y)

# recom_df["tags"] = recom_df["tags"].apply(stem)
# print(recom_df.head(5))

# similarity = cosine_similarity(vectors)

# def recommend(movie):
#     movie_index = recom_df[recom_df["title"] == movie].index[0]
#     distances = similarity[movie_index]
#     movies_list = sorted(list(enumerate(distances)), reverse=True, key=lambda x:x[1])[1:6]

#     for i in movies_list:
#         print(recom_df.iloc[i[0]].title)

# recommend("Iron Man")

spark.stop()