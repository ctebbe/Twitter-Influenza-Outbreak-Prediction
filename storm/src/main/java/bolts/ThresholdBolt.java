package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;
import util.Util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by ct.
 */
public class ThresholdBolt extends BaseRichBolt {

    PrintWriter writer;
    private final String filename;
    private OutputCollector collector;
    private int threshold;
    private int count;

    public ThresholdBolt(String filename) {
        this.filename = filename;
        count = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        threshold = (Integer) map.get("threshold");
        collector = outputCollector;
        try {
            writer = new PrintWriter(filename,"UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String text = tuple.getStringByField("message");
        writer.println(Util.getTimeStamp() + ":" + text);
        if(++count > threshold)
            writer.println("*** THRESHOLD EXCEEDED ***");
        writer.flush();
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}
