# Yelp project

This is a my yelp project I did at my university. I used sentiment analysis and Pyspark to analyze over a million reviews. 
There are two separate projects one was more machine learning focused.  The objective for the ‘yelp.py’ code was to find the top ten words for the negative reviews and the top ten words for the positive reviews for each restaurant genre. 

Here’s a quick walk through of my process. First, the data was cleaned and the required columns were selected. Then, the required columns were selected and the data was filtered by the genre type i.e. Mexican, Thai, etc. Once the data was separated, it was converted to an RDD. Next, I filtered the data on positive and negative reviews. I flatmapped through the RDDs and added a count. I reduced by key and sorted to find the top words. The indexed the sorted RDDs to show only the top ten. 

The objective for the machine learning code was to predict a genre of restaurant by only using the review text. 
