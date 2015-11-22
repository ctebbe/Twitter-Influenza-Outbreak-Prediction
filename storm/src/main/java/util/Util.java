package util;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ctebbe
 */
public class Util {

    public static String getTimeStamp() {
        return new SimpleDateFormat("HH:mm:ss:SS").format(new Date());
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    public static Config getEmitFrequencyConfig() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }
}
