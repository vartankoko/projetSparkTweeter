import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Created by kokoghlanian on 06/06/2018.
 */
public class TweetStream {

    public static boolean hashTagContains(String hashtag, HashtagEntity[] hashtagEntities){
        for(int i=0; i<hashtagEntities.length; i++){
            if(hashtagEntities[i].getText().equalsIgnoreCase(hashtag)){
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {

        final String consumerKey = "TyrYpLLdRtK3a4zjKK7m4HG9a";
        final String consumerSecret = "AFR0JDpDejDBQqAv6SLrDoDzuIoiTJHAT4o82ul4J3GLrqftJm";
        final String accessToken = "1004642883117477888-qdocsLrScezdEEkcgGljEgeUPnkUxm";
        final String accessTokenSecret = "nME90OrO4P9abkmSoL7nr0fD26ox0CIvV9qGnCbq2c41Q";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterHelloWorldExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        // Without filter: Output text of all tweets
        /*JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );*/

        JavaDStream<Status> tweetsWithHashTag = twitterStream.filter(
                new Function<Status, Boolean>() {
                    public Boolean call(Status status){
                        if (status.getHashtagEntities().length != 0 ){ //&& status.getLang().toString().equalsIgnoreCase("fr")) {
                            if(hashTagContains("#FelizJueves", status.getHashtagEntities())){
                                return true;
                            }
                            return false;
                        } else {
                            return false;
                        }
                    }
                }
        );
        JavaDStream<String> statuses = tweetsWithHashTag.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return status.getText() + " DATE : "+ status.getCreatedAt().toString();
                    }
                }
        );

       /* JavaDStream<String> words = twitterStream.flatMap(new FlatMapFunction<Status, String>() {
            public Iterable<String> call(Status status){
                return Arrays.asList(status.getText().split(" "));
            }
        });

        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
            public Boolean call(String word) {
                return word.startsWith("#");
            }
        });*/

        //hashTags.print();

        statuses.print();
        jssc.start();
    }
}
