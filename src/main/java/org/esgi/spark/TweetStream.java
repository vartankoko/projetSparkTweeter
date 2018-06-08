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

    //public static Map<String, Integer> mapRT = new HashMap<String, Integer>();
    //public static Map<String, Integer> mapMention = new HashMap<String, Integer>();



    /*public static void getTwitterMentions(UserMentionEntity[] mentionEntities){
        for (UserMentionEntity mentionEntity : mentionEntities) {
            if(mapMention.containsKey(mentionEntity.getName())){
                mapMention.put(mentionEntity.getName(), mapMention.get(mentionEntity.getName())+1);
            } else {
                mapMention.put(mentionEntity.getName(), 1);
            }
        }
    }*/

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

        // Without filter: Output text of all tweets
        /*JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );*/
        /*JavaDStream<Status> tweetsWithRT = twitterStream.filter(
                new Function<Status, Boolean>() {
                    public Boolean call(Status status){
                        if (status.getText() != null && status.isRetweet()){//status.getHashtagEntities().length != 0 && hashTagContains("ThursdayThoughts", status.getHashtagEntities())){ //&& status.getLang().toString().equalsIgnoreCase("fr")) {
                            String name = status.getRetweetedStatus().getUser().getName();
                            if(mapRT.containsKey(name)){
                                mapRT.put(name, mapRT.get(name)+1);
                            }else{
                                mapRT.put(name, 1);
                            }
                            System.out.println("MAPRT length :"+mapRT.size());
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
        );

        JavaDStream<Status> tweetsWithMention = twitterStream.filter(
                new Function<Status, Boolean>() {
                    public Boolean call(Status status){
                        if (status.getText() != null && status.getUserMentionEntities().length != 0){//status.getHashtagEntities().length != 0 && hashTagContains("ThursdayThoughts", status.getHashtagEntities())){ //&& status.getLang().toString().equalsIgnoreCase("fr")) {
                            getTwitterMentions(status.getUserMentionEntities());
                            System.out.println("MAPMENTION length :"+mapMention.size());
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
        );
        JavaDStream<String> statuses = tweetsWithRT.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return "";
                    }
                }
        );

        JavaDStream<String> statuses2 = tweetsWithMention.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return "";
                    }
                }
        );*/
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
        /*System.out.println("MAPRT");
        for (String s : mapRT.keySet()) {
            System.out.println(s + " : " + mapRT.get(s));
        }*/
        /*System.out.println("MAPMENTION");
        for (String s : mapMention.keySet()) {
            System.out.println(s + " : " + mapMention.get(s));
        }*/

        jssc.start();
        jssc.awaitTermination();
    }
}
