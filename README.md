# Big Data Project - Twitter Tweet/Hashtag Analysis/Prediction

This project uses Apache Flink and is implemented in Java.
The folowing Metrics were implemented for Stream and Batch case:
- Count of Hashtags in the tweet
- Count of Likes in the tweet
- Finding the longest word in the tweet
- Count the amount of Retweets for a tweet
- Count of Words in a tweet

All these metrics are applied to the tweets, that contain one of the following Hashtags:
-  "china"
-  "usa"
-  "russia"
-  "germany"
-  "ukraine"

### Visualisation
At first I tried to implement elasticsearch/kibana visualisation, but unfortunately
I wasn't very successful with it. Thus I decided to simply print and save as files all the output of my jobs.

### How to run
You need to import the pom.xml into Intellij and then select the job you want to run.

### What to start with
First of all, there is a debug mode, which allows you to simulate the Twitter-Stream 
with the data that have been collected. It can be changed by setting flag debug from
```
flinkProject/src/main/resources/JobConfig.properties
```
to "true" or something else. In case you set it as "true", the datastream will be simulated.
It is pretty handy, since there are not so many tweets that contain an explicit hashtag
but rather people just write i.e. china in the text. This is something I understood way too late 
to change the structure of the Project.

As next, you should setup your Twitter connection keys in 
```
flinkProject/src/main/resources/twitter.properties
```
Additionally, you can change the output/input properties of different jobs in 
```
flinkProject/src/main/resources/JobConfig.properties
```
If there is no generated data and you can't/won't run jobs online, you can generate 
your own data with the help of
```
flinkProject/src/main/java/org/bdcourse/tools/TwitterJsonCreator.java
```
It will generate the tweets (not completely legit ones, but these have all the properties needed for the jobs to run).

### What/and how to run the jobs after the setup
- **milestone 1**  Data collection for storage and analysis from twitter in real time

    For this one, you need to run the BatchCollector from the package stream.
    This job simply collects all the incoming tweets. 
    
- **milestone 2**  Online analysis of the selected hashtags (5 hashtags) for the selected metrics (5 metrics)

    For this one, you can run any of the classes in the package stream except for BatchCollector.
    I.e. you can take TwitterStreamHashtagCount,
                      TwitterStreamLikeCount,
                      TwitterStreamLongestWord,
                      TwitterStreamRetweetCount,
                      TwitterStreamWordCount
                      
- **milestone 3**  Offline analysis of the hashtags (same hashtags/metrics as for milestone 2)

    This is the same as milestone 2, except you can take any class from the package batch
    
- **milestone 4**  Online comparison of the online statistics with the offline ones

    Here it gets interesting. I implemented the comparison for HashtagCount, LikeCount and RetweetCount by using 
    the "RichCoFlatMapFunction". I have to mention, that I did change the approach here, as previously, I didn't take any average metric
    for the batch collected data. Here it did make more sense to at least reduce and get the average of the batch data, in order to make the 
    data more homogeneous. At first, I didn't make a use of "ValueState", which means,
    I would take a data point from the batch, save it as local variable inside of the process function and 
    then compare it with the streamed value. It only came later to my attention that this approach would
    break if we have really large amounts of data, and so I had to use the "ValueState". The problem with this approach, 
    that both streams don't seem to synchronise anymore and the case arises, where some portion of stream data
    could arrive before the batch data is there to be compared with. I couldn't find any option to force the streams to wait 
    for the other stream, but as the actual twitter-stream gets to run for some time, there should be always a batch value to compare
    it with. With these problems, I chose to release TWO versions of my comparison jobs, one without ValueState and one with ValueState.
    
- **milestone 5**  A non-constant prediction mechanism developed by the students (for 3 of the selected metrics)

    Analogous to the milestone 4, I also didn't make use of the ValueState here at first. thus I also released two versions of my 
    prediction jobs.
    I implemented one simple regression, which shall predict the amount of hashtags given the amount
    of words in a tweet. The regression is computed beforehand with the help of batch data, which means, at first the batch datastream is 
    processed and the output of the datastream if fed into the regression model. It delivers a tuple of four with 
        
        a) the amount of words in a tweet
        b) the amount of hashtags in a tweet
        c) the predicted integer amount of hashtags in a tweet
        d) the predicted decimal amount of hashtags in a tweet (in order to observe the direction of how the predicted value changes) 
    Another two prediction jobs compute the moving average of WordCount and HashtagCount. Analogous to the regression, 
    the batch data is processed first and then the amount of tweets and sum of the hashtags/words of tweets is computed and 
    then these data are being used to compute the average. It delivers a tuple of three with
        
        a) the corresponding hashtag
        b) the amount of Words/Hashtags
        c) the average before
        
    All predictions are being updated with each new data point from the stream.
    
### youtube link to my presentation
not present yet