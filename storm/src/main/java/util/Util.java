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

    public static final String REGEX = "/#flu|[F|f]lu|[I|i]nfluenza|[H|h][1|3|7][N|n][1|3|9]/g";

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
