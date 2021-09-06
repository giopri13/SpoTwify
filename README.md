# SpoTwify
A simple tool that looks for tweets sharing songs included in the [Top 50 Global Playlist](https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF) on Spotify and classifies them as positive or negative, making use of technologies such as Apache Kafka, Apache Spark, Elastic Search and Kibana. Developed as final project for the Technologies for Advanced Programming subject.


## Read Me

### Prerequisites:

Before using the tool, be sure to download and place the following items in the correct folders:
* Download [spark-3.1.1-bin-hadoop2.7.tgz](https://archive.apache.org/dist/spark/spark-3.1.1/) and put it in _/spark/setup/_
* Download [Twitter Sentiment Analysis Dataset](http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/) and put it in _/spark/dataset/_

Remember to run chmod -R 777 * from the main folder to avoid any issue due to missing permissions.

### How to Run

In order to run the tool, move to directory _/bin_ and run the following scripts in this order:

1. ./kafkaStartZk.sh
2. ./kafkaStartServer.sh
3. ./elasticSearch.sh
4. ./kibana.sh
5. ./sparkStart.sh
6. ./kafkaPythonDataGatherer.sh
