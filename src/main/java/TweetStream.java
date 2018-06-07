import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

import java.util.*;


/**
 * Created by kokoghlanian on 06/06/2018.
 */
public class TweetStream {

    public static Map<String, Integer> mapRT = new HashMap<String, Integer>();
    public static Map<String, Integer> mapMention = new HashMap<String, Integer>();

    public static void getTwitterMentions(UserMentionEntity[] mentionEntities){
        for (UserMentionEntity mentionEntity : mentionEntities) {
           if(mapMention.containsKey(mentionEntity.getName())){
                mapMention.put(mentionEntity.getName(), mapMention.get(mentionEntity.getName())+1);
            } else {
               mapMention.put(mentionEntity.getName(), 1);
            }
        }
    }

    public static void main(String[] args) {

        final String consumerKey = "TyrYpLLdRtK3a4zjKK7m4HG9a";
        final String consumerSecret = "AFR0JDpDejDBQqAv6SLrDoDzuIoiTJHAT4o82ul4J3GLrqftJm";
        final String accessToken = "1004642883117477888-qdocsLrScezdEEkcgGljEgeUPnkUxm";
        final String accessTokenSecret = "nME90OrO4P9abkmSoL7nr0fD26ox0CIvV9qGnCbq2c41Q";

        /*final String consumerKey = "Hszofr5hTx9zYaeioCRfGG3nP";
        final String consumerSecret = "AUk23YQHwv5dOdaTs4nwbxYogar0duwmHLWe2nps4Q1CrUkEkB";
        final String accessToken = "1004360784493924354-ijpJpluAnlbTOaZQ2OrFDLqMc0FAWZ";
        final String accessTokenSecret = "0ZPsxatgLn3waPI9Q9vWYjT4rjrRpWAtt0B5aClQNqht1";*/

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterHelloWorldExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, new String[]{"#ThursdayThoughts"});

        // Without filter: Output text of all tweets
        /*JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );*/
        JavaDStream<Status> tweetsWithRT = twitterStream.filter(
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
        );
       JavaDStream<String> words = twitterStream.flatMap(new FlatMapFunction<Status, String>() {
            public Iterable<String> call(Status status){
                return Arrays.asList(status.getText().split(" "));
            }
        });

        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
            public Boolean call(String word) {
                return word.startsWith("https://");
            }
        });

        //hashTags.print();

        statuses.print();
        statuses2.print();
        /*System.out.println("MAPRT");
        for (String s : mapRT.keySet()) {
            System.out.println(s + " : " + mapRT.get(s));
        }*/
        /*System.out.println("MAPMENTION");
        for (String s : mapMention.keySet()) {
            System.out.println(s + " : " + mapMention.get(s));
        }*/

        jssc.start();
    }
}
