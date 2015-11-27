import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolts.ThresholdBolt;
import spout.TwitterSpout;

/**
 * Created by ct.
 */
public class MainTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TwitterSpout());
        builder.setBolt("log", new ThresholdBolt("out/tweets.log")).shuffleGrouping("spout");

        Config conf = new Config();
        conf.put("threshold", args[0]);
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("twitter", conf, builder.createTopology());
    }
}
