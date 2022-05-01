from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.rdd import RDD
from pyspark.mllib.feature import StandardScaler, PCA
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import col, count, isnan, when
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import udf,col,split,translate,lower
from pyspark.sql.types import StringType
from pyspark.sql.functions import array_contains
import pyspark.sql.functions as func

conf = SparkConf().setMaster("local[*]").set("spark.executer.memory", "2g")

sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")






from pyspark import SparkConf, SparkContext
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords

#defining the stop words 
stopword_list = set(stopwords.words("english"))

stopwords = nltk.corpus.stopwords.words('english')
newStopWords = ['would','food','go','like','one','get','order','place','us']
stopwords.extend(newStopWords)



#preprocessing UDF
def ProcessText(text):
	tokens = nltk.word_tokenize(text)
	remove_punct = [word for word in tokens if word.isalpha()]
	remove_stop_words = [word for word in remove_punct if not word in stopwords]
	return remove_stop_words


#load and clean data

df = spark.read.csv('/data/yelp-dataset/yelp_review.csv',header = True)
#df.show(20)
print(df.columns)


df2 = spark.read.csv('/data/yelp-dataset/yelp_business.csv',header = True, inferSchema = True)
#df2.show(20)
print(df2.columns)

#drop unwanted columns before join 
list_ = ('neighborhood', 'address','stars', 'city','state', 'postal_code', 'latitude', 'longitude', 'review_count', 'is_open','review_id', 'user_id', 'date', 'useful', 'funny', 'cool')
df2 = df2.drop(*list_)




# join dataframes on bus id
df = df.join(df2, on=['business_id'], how='inner')


#limit df to debug locally
#df = df.limit(1000)

#drop columns
#list2 = ('neighborhood', 'address', 'city', 'stars','state', 'postal_code', 'latitude', 'longitude', 'review_count', 'is_open','review_id', 'user_id', 'date', 'useful', 'funny', 'cool')
#df = df.drop(*list2)

#select wanted columns 

df = df.select('stars','text', 'categories')
#look at datafame 
#df.show(10)

#check types of data
#df.printSchema()

#print(df.columns)




##preprocessing 
df = df.withColumn("stars",col("stars").cast("int"))
#code to change data type in columns 
#df = df.select(*(col(c).cast('float').alias(c) for c in dataset.columns)) 

# drop any and all nulls 
#drop_missing = df.dropna(how='any')
#drop_missing = df.dropna(how='all')

#there are no nulls for string types
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

#df.printSchema()

#df.show(30)

#count the categorgies

df.groupBy('stars').count().show()

#EDA
#df_genres = df.select('categories')

#df_genres.show()

#shape of dataset


#descibe
#df.describe().show()

#tokenize words





udf_lower_clean_str = udf(lambda x:ProcessText(x),StringType() )

df = df.withColumn("categories",udf_lower_clean_str(col("categories")))#.select("text","categories").distinct().show(5)



#lower 
df = df.withColumn("text",func.lower(func.col("text")))

#df.show()
#df.printSchema()



def udf_rating(rating): 
    if int(rating >=4):
        return "Positive review"
    elif rating ==3 :
        return "Somewhat Positive"
    else:
        return "Negative review"
rating_udf = udf(udf_rating)

df = df.withColumn("Rating_label", rating_udf('stars'))


#df.show(20)



Thai = df.filter(df.categories.contains("Thai"))#.show()





#filter approach 



## thai - 5star
rdd=Thai.rdd
thai = rdd.filter(lambda x: x[3]== "Positive review")#.collect()

words = thai.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews1 = result.map(lambda x:(x[0],x[1]))



thai1 = rdd.filter(lambda x: x[3]== "Negative review")#.collect()

words = thai1.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews2 = result.map(lambda x:(x[0],x[1]))






Mexican = df.filter(df.categories.contains("Mexican"))#.show()





#filter approach 



## Mexican - 5star
rdd= Mexican.rdd
mexican = rdd.filter(lambda x: x[3]== "Positive review")#.collect()

words = mexican.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews3 = result.map(lambda x:(x[0],x[1]))




mexican1 = rdd.filter(lambda x: x[3]== "Negative review")#.collect()

words = mexican1.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews4 = result.map(lambda x:(x[0],x[1]))






#filter for Indian positive first then negative

Indian = df.filter(df.categories.contains("Indian"))#.show()
rdd= Indian.rdd
indian = rdd.filter(lambda x: x[3]== "Positive review")#.collect()

words = indian.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews5 = result.map(lambda x:(x[0],x[1]))




indian1 = rdd.filter(lambda x: x[3]== "Negative review")#.collect()

words = indian1.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews6 = result.map(lambda x:(x[0],x[1]))



#filter for chinese food positive first then negative
Chinese = df.filter(df.categories.contains("Chinese"))#.show()

rdd= Chinese.rdd
chinese = rdd.filter(lambda x: x[3]== "Positive review")#.collect()

words = chinese.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews7 = result.map(lambda x:(x[0],x[1]))



chinese1 = rdd.filter(lambda x: x[3]== "Negative review")#.collect()

words = chinese1.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews8 = result.map(lambda x:(x[0],x[1]))


#filter for american food positive first then negative 
American = df.filter(df.categories.contains("American"))#.show()

rdd= American.rdd
american = rdd.filter(lambda x: x[3]== "Positive review")#.collect()

words = american.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews9 = result.map(lambda x:(x[0],x[1]))



american1 = rdd.filter(lambda x: x[3]== "Negative review")#.collect()

words = american1.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews10 = result.map(lambda x:(x[0],x[1]))

#pizza

Pizza = df.filter(df.categories.contains("Pizza"))#.show()

rdd= Pizza.rdd
Pizza = rdd.filter(lambda x: x[3]== "Positive review")#.collect()

words = Pizza.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews11 = result.map(lambda x:(x[0],x[1]))



pizza1 = rdd.filter(lambda x: x[3]== "Negative review")#.collect()

words = pizza1.flatMap(lambda x:ProcessText(x[1]))

inter_rdd = words.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
#result_collect = result#.collect()

reviews12 = result.map(lambda x:(x[0],x[1]))


# organize to print next to each other 
thai1 ='Thai Positive:', sorted(reviews1.collect(), key = lambda x:x[1],reverse = True)[:15]


thai2= 'Thai Negative:', sorted(reviews2.collect(), key = lambda x:x[1],reverse = True)[:15]


mex1 = 'Mexican Positive:', sorted(reviews3.collect(), key = lambda x:x[1],reverse = True)[:15]

mex2 = 'Mexican Negative:', sorted(reviews4.collect(), key = lambda x:x[1],reverse = True)[:15]



indian1 = 'Indian Positive:', sorted(reviews5.collect(), key = lambda x:x[1],reverse = True)[:10]

indian2 = 'Indian  Negative:', sorted(reviews6.collect(), key = lambda x:x[1],reverse = True)[:10]

chinese1 = 'Chinese Positive:', sorted(reviews7.collect(), key = lambda x:x[1],reverse = True)[:15]


chinese2 = 'Chinese Negative:', sorted(reviews8.collect(), key = lambda x:x[1],reverse = True)[:15]


american1 = 'American Positive:', sorted(reviews9.collect(), key = lambda x:x[1],reverse = True)[:15]


american2 = 'American Negative:', sorted(reviews10.collect(), key = lambda x:x[1],reverse = True)[:15]

pizza1 = 'Pizza Positive:', sorted(reviews11.collect(), key = lambda x:x[1],reverse = True)[:10]


pizza2 = 'Pizza Negative:', sorted(reviews12.collect(), key = lambda x:x[1],reverse = True)[:10]


### info for cloud 

from pyspark.sql.types import StructType
#columns = ['review type','top words']
#userSchema = StructType().add("review type", "string").add("top words", "string")
#dataframe = spark.createDataFrame(pizza1,StringType(), columns)#.schema(userSchema)

#rdd_union = sc.union([reviews1, reviews2, reviews3])
##rdd_union.saveAsTextFile("test.txt")
#dataframe.head()

## save on cloud 
thai_list_pos = sc.parallelize(list(thai1))
thai_list_neg = sc.parallelize(list(thai2))
mex_list_pos = sc.parallelize(list(mex1))
mex_list_neg = sc.parallelize(list(mex2))
indian_list_pos = sc.parallelize(list(indian1))
indian_list_neg = sc.parallelize(list(indian2))
chinese_list_pos = sc.parallelize(list(chinese1))
chinese_list_neg =sc.parallelize(list(chinese2))
american_list_pos =sc.parallelize(list(american1))
american_list_neg = sc.parallelize(list(american2))
pizza_list_pos= sc.parallelize(list(pizza1))
pizza_list_neg = sc.parallelize(list(pizza2))
final = sc.union([thai_list_pos, thai_list_neg, mex_list_pos,mex_list_neg,indian_list_pos,indian_list_neg,chinese_list_pos,chinese_list_neg,american_list_pos,american_list_neg,pizza_list_pos,pizza_list_neg])
final.saveAsTextFile('/data/final4')

### how to print locall



