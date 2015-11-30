package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import util.Util;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by ct.
 */
public class TwitterSpout extends BaseRichSpout {

    public static final String consumerKey = "n4yQ2ltlZ7VBlNbYj217JYDIj";
    public static final String consumerSecret = "0BFeKPqo4ssB91Hlu9X0gxhH4Hj7JrZNN0IHXW3MXZCHejTOdn";
    public static final String accessToken = "353264724-pFaJBAR5sWacJKEyATOcrFC7WV2HAmX6QxBUlBNd";
    public static final String accessTokenSecret = "A14UR52qk8OUzjPynZHodlitJ06IOyi7kZA3HSqI9qNUk";

    private SpoutOutputCollector collector;
    private TwitterStream twStream;
    private LinkedBlockingQueue msgs;

    public TwitterSpout() {
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        msgs = new LinkedBlockingQueue();
        collector = spoutOutputCollector;
        ConfigurationBuilder confBuilder = new ConfigurationBuilder();
        confBuilder.setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret)
                .setJSONStoreEnabled(true);
        twStream = new TwitterStreamFactory(confBuilder.build()).getInstance();

        twStream.addListener(new StatusListener() {
            public void onStatus(Status status) {
                GeoLocation loc = status.getGeoLocation();
                if(loc != null) {
                    if(loc.getLongitude() > 105
                            && loc.getLongitude() < 135
                            && status.getText().matches(Util.REGEX)) {
                        msgs.offer(status.getText());
                    }
                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception e) {

            }
        });
        twStream.sample();
    }

    public void nextTuple() {
        Object s = msgs.poll();
        if(s == null) {
            Utils.sleep(1000);
        } else {
            collector.emit(new Values(s));
        }
    }

    public void close() {
        twStream.shutdown();
        super.close();
    }
}
