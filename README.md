# Yelp project


### Project Intro 
Yelp has provided a platform where anyone can be a food critic. Yelp allows everyday food goers the ability to rate and review a business. Many people will check the review section of Yelp or their preferred search engine before deciding on a place to eat. In fact, these reviews are so powerful they can affect your business such as a negative review that can cause revenues to drop and a positive review that can cause a rush of new customers. 
When reviewing, culture plays a large part on how people build opinions towards different types of foods and cuisines [1]. There are many dishes people will drive miles out of their way to get. Identifying these dishes and significant moments in customers' reviews, can help ethnic cuisine market to their local communities. This report will explore the different opinions expressed in a genre's positive and negative reviews. Businesses can utilize the opinions of others to shape their community outreach. This sentiment analysis will provide businesses better insights on their strengths and weaknesses so they know how to grow their revenue and make improvements.


### Dataset and Systems

Sentiment analysis is a popular tool used to turn review data into information that can improve their business. Sentiment analysis is the process of computing text data to identify the emotional tone behind it [2]. It uses natural language processing models to process data retrieved from online forums. The datasets used for this sentiment analysis are from the Yelp review section and combine are over 4 gigabytes. They are two csv files joined during the data preparation for the analysis. 

Due to the size of these datasets, these models are built and executed on the Apache Spark platform. The project will use multiple Spark APIs and libraries to analyze the data such as Pyspark, Spark ML, NLTK (Natural Language Toolkit), RDD, Pyspark Dataframe, and multiple SQL libraries. These were all run on a local terminal and the Google Cloud Platform. There are three code files attached to this report that support our analysis. 
