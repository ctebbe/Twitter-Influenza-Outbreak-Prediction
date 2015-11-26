package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu on 11/7/15.
 */
public class ParserReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int matchCount = 0;
        int totalCount = 0;
        for (Text value : values) {
            String[] vals = value.toString().split(":");

            matchCount += Integer.parseInt(vals[0]);
            totalCount += Integer.parseInt(vals[1]);
        }
        context.write(key, new Text(matchCount + ":" + totalCount));
    }
}
