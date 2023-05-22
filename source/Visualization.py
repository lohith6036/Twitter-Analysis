from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import explode
import  matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, send_file, render_template, request
from matplotlib.figure import Figure
import pandas as pd
import seaborn as sns

app = Flask(__name__)
@app.route("/Query/<qid>", methods=['GET'])
def hello(qid):
 id = int(qid)
 if(id == 1):
     query1 = spark.sql("select count(*) as NumberOfTweets, 'Android' as Source from TweetsFile where source like '%Twitter for Android%' UNION select count(*) as NumberOfTweets, 'IPhone' as Source from TweetsFile where source like '%Twitter for iPhone%' UNION select count(*) as NumberOfTweets, 'IPad' as Source from TweetsFile where source like '%Twitter for iPad%' UNION select count(*) as NumberOfTweets, 'Web' as Source from TweetsFile where source like '%Twitter Web App%'")
     pd1 = query1.toPandas()
     img1 = BytesIO()
     pd1.plot.pie(y='NumberOfTweets',labels=['IPhone', 'Web','Android','IPad'],autopct='%.2f',figsize=(5,5),title="Source of Tweets")
     plt.savefig(img1)
     img1.seek(0)
     return send_file(img1, mimetype="image/png")

 if(id ==2):
     query2 = spark.sql("select '-1 to -0.5' as score, count(*) as tweets from TweetsFile where text like '%Republican%' or text like'Republics' or text like'%GOP%' and pol between -1 and -0.5 UNION select '-0.5 to 0.5' as score,count(*) as tweets from TweetsFile where text like '%Republican%' or text like'Republics' or text like'%GOP%' and pol between -0.5 and 0.5 UNION select '0.5 to 1' as score,count(*) as tweets from TweetsFile where text like '%Republican%' or text like'Republics' or text like'%GOP%' and pol between 0.5 and -1")
     pd2 = query2.toPandas()
     pd2.plot(kind="area", x="score", y="tweets", title="Republicans polarity score range")
     img2 = BytesIO()
     plt.savefig(img2)
     img2.seek(0)
     return send_file(img2, mimetype="image/png")

 if(id == 3):
     query3 = spark.sql(
         "select count(*) as tweets, 'Republic' as Party from TweetsFile where text like '%Republican%' or text like'Republics' or text like '%Republican Party%' or text like'%GOP%' and pol = 1 UNION select count(*) as tweets, 'Democratic' as Party from TweetsFile where text like '%Democrat%' or text like '%democratic%' or text like '%Democratic Party%' and pol = 1")

     pd3 = query3.toPandas()
     pd3.plot(kind="bar", x="Party", y="tweets", title="Republicans and Democratic tweets with 100% positivity")
     img3 = BytesIO()
     plt.savefig(img3)
     img3.seek(0)
     return send_file(img3, mimetype="image/png")

 if(id == 4):
     query4 = spark.sql(
         "select count(*) as tweets,'English' as language from TweetsFile where lang = 'en' UNION select count(*) as tweets,'Spanish' as language from TweetsFile where lang = 'es' UNION select count(*) as tweets,'Turkish' as language from TweetsFile where lang = 'tr'")
     pd4 = query4.toPandas()
     # pd.plot.area(x = "language",y="tweets",data=pd)
     pd4.plot.area(x='language',y='tweets',title="Languages with tweets count")
     img4 = BytesIO()
     plt.savefig(img4)
     img4.seek(0)
     return send_file(img4, mimetype="image/png")

 if(id == 5):
     query5 = spark.sql(
         "select count(*) as count, place.country from TweetsFile where place.country is not null group by place.country order by count desc limit 10")
     pd5 = query5.toPandas()
     pd5.plot.area(x="country", y="count", title="Tweets from different countries")
     img5 = BytesIO()
     plt.savefig(img5)
     img5.seek(0)
     return send_file(img5, mimetype="image/png")


 if(id == 6):
     query6 = spark.sql(
         "select  avg(pol) as polarity, 'Joe' as candidate from TweetsFile where text like '%@JoeBiden%' UNION select avg(pol) as polarity,'Trump' as candidate from TweetsFile where text like '%@realDonaldTrump%'")
     pd6 = query6.toPandas()
     # sns.catplot(x='candidate', y='polarity',data=pd6)
     # plt.title("")
     # pd6.plot(kind='line', x='candidate', y='polarity', color='red', ax=ax)
     sns.catplot(x='candidate', y='polarity', data=pd6).set(title='Trump and Joe avarage polarity')
     img6 = BytesIO()
     plt.savefig(img6)
     img6.seek(0)
     return send_file(img6, mimetype="image/png")


 if(id == 7):
     query7 = spark.sql(
         "select user.name as user_name,retweeted_status.retweet_count as retweet_count from TweetsFile order by  retweeted_status.retweet_count desc limit 5")
     pd7 = query7.toPandas()
     # pd.plot(kind='bar',x='user_name',y="retweet_count")
     pd7.plot.pie(y="retweet_count", labels=pd7.user_name.tolist(),autopct='%.2f',
                 title='Top 5 users with highest number of retweets')

     img7 = BytesIO()
     plt.savefig(img7)
     img7.seek(0)
     return send_file(img7, mimetype="image/png")

 if(id == 8):
     query8 = spark.sql(
         "select user.name,user.followers_count from TweetsFile order by user.followers_count desc limit 10").dropDuplicates()
     query8.createOrReplaceTempView("df1")
     subQuery8 =spark.sql(
         "SELECT name,followers_count FROM (SELECT *, MAX(followers_count) OVER (PARTITION BY name) AS maxB FROM df1) M WHERE followers_count = maxB")
     pd8 = query8.toPandas()
     pd8.plot(x="name", y="followers_count",title='Users with maximum number of followers',figsize=(10,5))
     img8 = BytesIO()
     plt.savefig(img8)
     img8.seek(0)
     return send_file(img8, mimetype="image/png")


 if(id == 9):
     query9 = spark.sql(
         "select count(*) as tweets,'Republicans' as Party from TweetsFile where text like '%Republican%' or text like'Republics' or text like '%Republican Party%' or text like'%GOP%' and subjectivity = 1 UNION select count(*) as tweets,'Democrats' as Party from TweetsFile where text like '%Democrat%' or text like '%democratic%' or text like '%Democratic Party%' and subjectivity = 1")
     pd9 = query9.toPandas()
     sns.catplot(x="Party", y="tweets",kind="point",data=pd9)
     img9 = BytesIO()
     plt.savefig(img9)
     img9.seek(0)
     return send_file(img9, mimetype="image/png")

 if(id == 10):
     query10 = spark.sql(
         "select user.location,count(text) as tweet_count from TweetsFile where place.country='United States'and "
         "user.location is not null and user.location != 'United States' Group By user.location ORDER BY tweet_count DESC LIMIT 5")
     pd10 = query10.toPandas()
     pd10.plot(linestyle='-.',title="Top 5 states with highest number of tweets",x="location",y="tweet_count",figsize=(8,5))
     img10 = BytesIO()
     plt.savefig(img10)
     img10.seek(0)
     return send_file(img10, mimetype="image/png")


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()
    # spark is an existing SparkSession
    df = spark.read.json(r"C:\Users\jagad\PycharmProjects\BDProject\TwitterStreamer\StreamApp\project_tweets_file.json")
    df.withColumn("pol", expr("CAST(pol AS INTEGER)"))
    df.withColumn("subjectivity", expr("CAST(subjectivity AS INTEGER)"))
    df.createOrReplaceTempView("TweetsFile")
    app.run(debug=True)

