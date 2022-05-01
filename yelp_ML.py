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
from pyspark.ml.feature import Tokenizer, HashingTF, IDF 
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql.functions import rand 


sc = SparkContext("local[*]")
spark = SparkSession(sc)



from pyspark import SparkConf, SparkContext
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords

#defining the stop words 
stopword_list = set(stopwords.words("english"))

stopwords = nltk.corpus.stopwords.words('english')
newStopWords = ['This','I','The','It']
stopwords.extend(newStopWords)



#preprocessing UDF
def ProcessText(text):
	tokens = nltk.word_tokenize(text)
	remove_punct = [word for word in tokens if word.isalpha()]
	remove_stop_words = [word for word in remove_punct if not word in stopwords]
	return remove_stop_words


#load and clean data

df = spark.read.csv('yelp-dataset/yelp_review.csv',header = True)
#df.show(20)
print(df.columns)


df2 = spark.read.csv('yelp-dataset/yelp_business.csv',header = True, inferSchema = True)
#df2.show(20)
print(df2.columns)

#drop unwanted columns before join 
list_ = ('neighborhood', 'address','stars', 'city','state', 'postal_code', 'latitude', 'longitude', 'review_count', 'is_open','review_id', 'user_id', 'date', 'useful', 'funny', 'cool')
df2 = df2.drop(*list_)




# join dataframes on bus id
df = df.join(df2, on=['business_id'], how='inner')


#limit df to debug locally
df = df.limit(1000)

#drop columns
#list2 = ('neighborhood', 'address', 'city', 'stars','state', 'postal_code', 'latitude', 'longitude', 'review_count', 'is_open',, 'user_id', 'date', 'useful', 'funny', 'cool')
#df = df.drop(*list2)

#select wanted columns 

df = df.select('review_id','text', 'categories')
#look at datafame 
#df.show(10)

#check types of data
#df.printSchema()

#print(df.columns)




##preprocessing 
#df = df.withColumn("stars",col("stars").cast("int"))
#code to change data type in columns 
#df = df.select(*(col(c).cast('float').alias(c) for c in dataset.columns)) 

# drop any and all nulls 
#drop_missing = df.dropna(how='any')
#drop_missing = df.dropna(how='all')

#there are no nulls for string types
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

df.printSchema()

#df.show(30)

#count the categorgies

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
df.printSchema()




#df.show(20)


from pyspark.sql.functions import lit

#add a label column for each category

thai = df.filter(df.categories.contains("Thai")).withColumn("label", lit('Thai'))#.show()

#thai.show()

mexican = df.filter(df.categories.contains("Mexican")).withColumn("label", lit('Mexican'))#.show()

#mexican.show()

bakery = df.filter(df.categories.contains("Bakeries")).withColumn("label", lit('Bakery'))#.show()
#bakery.show()
bakery = df.filter(df.categories.contains("Bakeries")).withColumn("label", lit('Bakery'))#.show()
#bakery.show()

bar = df.filter(df.categories.contains("American")).withColumn("label", lit('American'))#.show()
#bar.show()

chinese = df.filter(df.categories.contains("Chinese")).withColumn("label", lit('Chinese'))#.show()
#chinese.show()

pizza = df.filter(df.categories.contains("Pizza")).withColumn("label", lit('Pizza'))#.show()
#pizza.show()

#append each dataframe together
df1 = mexican.union(thai).union(bakery).union(bar).union(chinese).union(pizza)
                                                
# EDA each column
df1.groupBy("label").count().show()

#convert cat label to numeric for model
def label_convert(label): 
    if label == 'Thai':
        return 1
    elif label == 'Mexican':
        return 2
    elif label == 'Bakery':
        return 3
    elif label == 'American':
        return 4
    elif label == 'Chinese':
        return 5
    else:
        return 6

label_udf = udf(label_convert)

df1 = df1.withColumnRenamed('label', 'label_cat')
df1 = df1.withColumn("label", label_udf('label_cat'))


df = df1.select('text', 'label_cat', 'label')
#df = df.withColumnRenamed('label_convert(label)', 'label_num')
df = df.withColumn("label",col("label").cast("int"))
#df.show()

#shuffle data
import pandas as pd
# shuffle with pandas - shuffles well but creates error when fitting model 
#df = df.toPandas()

#df =  df.sample(frac=1).reset_index(drop=True) 
#df = spark.createDataFrame(df)


#shuffle with spark - does not shuffle well 
df = df.orderBy(rand())#.show(20)

 

df.show(20)

#test train split 
splits = df.randomSplit([0.6, 0.4],42)
train = splits[0]
test = splits[1]

#train.show()
#test.show()

#####################
tokenizer = Tokenizer(inputCol = "text", outputCol = "words") 
TF = HashingTF(inputCol = tokenizer.getOutputCol(), outputCol = "tfFeatures")
idf = IDF(inputCol = "tfFeatures",outputCol = "features")
lr = LogisticRegression(maxIter = 10, regParam = 0.001)

pipeline = Pipeline(stages = [tokenizer,TF,idf,lr])
model = pipeline.fit(train)

#####################################



test = test.select('text','label_cat').limit(100)


#Prediction
prediction = model.transform(test)

rows_looped = prediction.select('text','label_cat','prediction').collect()
number = 1
for rows in rows_looped:
	print('Comment '+ rows[0],'prediction'+ rows[1])
	print(" Comment (%i) - (%s) --> prediction = %s" %(number,rows[0],rows[1] ))
	number = number + 1



