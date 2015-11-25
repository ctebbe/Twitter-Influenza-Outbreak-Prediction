package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Qiu on 11/7/15.
 */
public class ParserMapper extends Mapper<Text, Text, Text, IntWritable> {

    public static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String dateStr = key.toString();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd");
        try {
            Date date = dateFormat.parse(dateStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH);
            int week = cal.get(Calendar.WEEK_OF_YEAR);
            if (month == 11 && week == 1) {
                year += 1;
            }
            Text newKey = new Text(year + "-" + week);
            context.write(newKey, ONE);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
