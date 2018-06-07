import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.Status;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Created by kokoghlanian on 06/06/2018.
 */
public class TweetStream {

    public static void main(String[] args) {

        final String consumerKey = "Hszofr5hTx9zYaeioCRfGG3nP";
        final String consumerSecret = "AUk23YQHwv5dOdaTs4nwbxYogar0duwmHLWe2nps4Q1CrUkEkB";
        final String accessToken = "1004360784493924354-ijpJpluAnlbTOaZQ2OrFDLqMc0FAWZ";
        final String accessTokenSecret = "0ZPsxatgLn3waPI9Q9vWYjT4rjrRpWAtt0B5aClQNqht1";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterHelloWorldExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        // Without filter: Output text of all tweets
        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );

        JavaDStream<String> words = twitterStream.flatMap(new FlatMapFunction<Status, String>() {
            public Iterable<String> call(Status status) throws Exception {
                return Arrays.asList(status.getText().split(" "));
            }
        });

        words.print();

        statuses.print();
        jssc.start();
    }
}
