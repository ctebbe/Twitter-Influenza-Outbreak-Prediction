package input;

import main.TwitterParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Qiu on 11/25/2015.
 */
public class ArchiveReader extends RecordReader<Text, Text> {

    private final TaskAttemptContext context;
    private String filename;
    private LineReader lineReader;
    private long start, end, currentPos;
    private Text currentLine = new Text("");
    private JSONObject json;
    //    private String hashtagstr = "";
    private Path path;
    private int tweetsSkipped;
    private boolean eof;
    private String date;

    public ArchiveReader(TaskAttemptContext context) {
        this.context = context;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration configuration = context.getConfiguration();

        this.path = split.getPath();
        this.filename = path.getName();

        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
//
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(configuration);
//        throw (new IOException(String.valueOf(codec.getConf()==null)));
        CompressionInputStream compressionInputStream = codec.createInputStream(inputStream);
        lineReader = new LineReader(compressionInputStream, configuration);

//        //initial start point and end point
        start = split.getStart();
        end = start + split.getLength();
        currentPos = start;
//        compressionInputStream.(start);
        if (start != 0) {
            start += lineReader.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
        }

        start += lineReader.readLine(currentLine);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (eof) {
            return false;
        }
        if (!this.filename.endsWith(".bz2")) {
            return false;
        }
        String grandparentFolder = this.path.getParent().getParent().getName();
//        throw new IOException(file);
        if (!grandparentFolder.matches("\\d{2}")) {
            return false;
        }


        int b = lineReader.readLine(currentLine);
//        throw (new IOException(String.valueOf(currentLine)));
        currentPos += b;
//        hashtagstr = "";
        while (b != 0) {
            json = new JSONObject(currentLine.toString());
            try {
                JSONArray hashtags = json.getJSONObject("entities").getJSONArray("hashtags");

                String timezone = json.getJSONObject("user").getString("time_zone");

                String[] datetime = json.getString("created_at").split(" ");
                date = datetime[5] + "-" + datetime[1] + "-" + datetime[2];

//                if (timezone.equals(TwitterParser.TIME_ZONE)) {//!text.matches(TwitterParser.REGEX)){ //&& timezone.equals(TwitterParser.TIME_ZONE)) {

//                throw (new IOException(String.valueOf(json)));
                for (int i =0; i < hashtags.length();i++) {
                    JSONObject hashtag = (JSONObject) hashtags.get(i);

                    if ((hashtag.getString("text").matches(TwitterParser.REGEX1) ||
                            hashtag.getString("text").matches(TwitterParser.REGEX2)) &&
                            timezone.equals(TwitterParser.TIME_ZONE)) {
//                    throw (new IOException(text));
//                    hashtagstr += "<" + ((JSONObject) iterator.next()).getString("text") + ">";
                        return true;
                    } else if (timezone.equals(TwitterParser.TIME_ZONE)) {
                        tweetsSkipped++;
                    }
                }

            } catch (JSONException e) {
//                throw (new IOException(String.valueOf(currentLine)));
//
//                b = lineReader.readLine(currentLine);
//                currentPos += b;
//                continue;
            } finally {
                b = lineReader.readLine(currentLine);
                currentPos += b;
            }
//            b = lineReader.readLine(currentLine);
//            currentPos += b;
        }

        return eof = true;
    }


    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {

        return new Text(date);
//        return new Text(hashtagstr);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
//        return new Text(json.getString("text"));
//        return new Text(hashtagstr);
        String tweets2 = String.valueOf(tweetsSkipped);
        tweetsSkipped = 0;
        if (eof) {
            return new Text(String.valueOf(0) + ":" + tweets2);
        }
        return new Text(String.valueOf(1) + ":" + tweets2);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

}
