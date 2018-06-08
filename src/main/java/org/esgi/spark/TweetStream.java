package org.esgi.spark;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;


/**
 * Created by kokoghlanian on 06/06/2018.
 */
public class TweetStream implements Serializable{

    public static void main(String[] args) {

        Accumulator<Integer>  cptTwitter ;
        Accumulator<Integer> cptOther;
        final String consumerKey = "TyrYpLLdRtK3a4zjKK7m4HG9a";
        final String consumerSecret = "AFR0JDpDejDBQqAv6SLrDoDzuIoiTJHAT4o82ul4J3GLrqftJm";
        final String accessToken = "1004642883117477888-qdocsLrScezdEEkcgGljEgeUPnkUxm";
        final String accessTokenSecret = "nME90OrO4P9abkmSoL7nr0fD26ox0CIvV9qGnCbq2c41Q";

        /*final String consumerKey = "Hszofr5hTx9zYaeioCRfGG3nP";
        final String consumerSecret = "AUk23YQHwv5dOdaTs4nwbxYogar0duwmHLWe2nps4Q1CrUkEkB";
        final String accessToken = "1004360784493924354-ijpJpluAnlbTOaZQ2OrFDLqMc0FAWZ";
        final String accessTokenSecret = "0ZPsxatgLn3waPI9Q9vWYjT4rjrRpWAtt0B5aClQNqht1";*/

        SparkConf conf = new SparkConf().setAppName("TwitterStreamNicolasYousriaVartan");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));
        cptTwitter = jssc.sparkContext().accumulator(0);
        cptOther = jssc.sparkContext().accumulator(0);
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, new String[]{args[0]});


        JavaDStream<String> words = twitterStream.flatMap(new FlatMapFunction<Status, String>() {
            @Override
            public Iterable<String> call(Status status) throws Exception {
                List<String> urls = new ArrayList<>();
                URLEntity[] urlEntities = status.getURLEntities();
                for (URLEntity urlEntity : urlEntities) {
                    String url = urlEntity.getExpandedURL();
                    urls.add(url);
                }
                //return Arrays.asList(status.getText().split(" "));
                return urls;
            }
        });

        JavaDStream<String> links = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String word) throws Exception {
                if (word != null && !word.equals("")) {
                    if (word.contains("https://twitter.com")) {
                        cptTwitter.add(1);
                    } else {
                        cptOther.add(1);
                    }
                    return false;

                }
                return false;
            }
        });

        links.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                System.out.println("TWITTER : " + cptTwitter.value() + " OTHER : " + cptOther.value());

            }
        });

        links.print();

        //statuses.print();
        //statuses2.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
