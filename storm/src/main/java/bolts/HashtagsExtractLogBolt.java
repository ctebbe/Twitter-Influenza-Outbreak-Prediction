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
public class HashtagsExtractLogBolt extends BaseRichBolt {

    PrintWriter writer;
    private final String filename;
    private OutputCollector collector;

    public HashtagsExtractLogBolt(String filename) {
        this.filename = filename;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
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
        StringTokenizer st = new StringTokenizer(text);
        StringBuilder sb = new StringBuilder();
        while(st.hasMoreElements()) {
            String tmp = (String) st.nextElement();
            if(StringUtils.startsWith(tmp, "#")) { // extract hashtags
                sb.append(tmp);
            }
        }
        if(sb.length() > 0) {
            writer.println(Util.getTimeStamp() +":"+ sb.toString());
            writer.flush();
        }
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
