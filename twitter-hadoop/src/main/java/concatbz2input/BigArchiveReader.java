package concatbz2input;

import main.TwitterParser;
import mapreduce.ParserMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by Qiu on 12/1/2015.
 */
public class BigArchiveReader extends RecordReader<Text, Text> {

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long end;
    private String filename;
    private LineReader lineReader;
    private FSDataInputStream fSDataInputStream;
    private TaskAttemptContext context;

    private Text currentLine = new Text("");
    private boolean validBzFile, eof;
    private JSONObject json;
    private String date = "";
    private int tweetsSkipped;
    private SplitCompressionInputStream compressionInputStream;
    private long counter = 0;


    public BigArchiveReader(FileSplit split, TaskAttemptContext context) throws IOException {

        this.context = context;
        Configuration conf = context.getConfiguration();
        start = split.getStart();
        end = start + split.getLength();
        Path path = split.getPath();
        this.filename = path.getName();

        FileSystem fileSystem = path.getFileSystem(conf);
        fSDataInputStream = fileSystem.open(path);
        fSDataInputStream.seek(start);

        Log log = LogFactory.getLog(ParserMapper.class);
        log.info(filename + ":" + start);
        System.out.println(filename + ":" + start + ":" + split.getLength());
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(conf);
        Decompressor decompressor = codec.createDecompressor();
        compressionInputStream = codec.createInputStream(fSDataInputStream, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);

//        compressionInputStream = codec.createInputStream(fSDataInputStream);
//        compressionInputStream.seek(start);
        lineReader = new LineReader(compressionInputStream, conf);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
        start += lineReader.readLine(currentLine);
        } catch (IOException e) {
            validBzFile = false;
            return;
        }
        validBzFile = true;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!validBzFile) {
            return false;
        }
        if (eof) {
            return false;
        }
        if (!this.filename.endsWith(".bz2")) {
            return false;
        }

        int b = lineReader.readLine(currentLine);
        while (b != 0 && compressionInputStream.getPos() < end) {

            counter++;
            if (counter % 1000 == 0) {
                context.progress();
            }

            try {
                json = new JSONObject(currentLine.toString());
                JSONArray hashtags = json.getJSONObject("entities").getJSONArray("hashtags");

                String timezone = json.getJSONObject("user").getString("time_zone");

                String[] datetime = json.getString("created_at").split(" ");
                date = datetime[5] + "-" + datetime[1] + "-" + datetime[2];
                for (int i = 0; i < hashtags.length(); i++) {
                    JSONObject hashtag = (JSONObject) hashtags.get(i);

                    if ((hashtag.getString("text").matches(TwitterParser.REGEX1) ||
                            hashtag.getString("text").matches(TwitterParser.REGEX2)) &&
                            timezone.equals(TwitterParser.TIME_ZONE) && !date.equals("")) {
                        return true;
                    } else if (timezone.equals(TwitterParser.TIME_ZONE)) {
                        tweetsSkipped++;
                    }
                }


            } catch (JSONException ignored) {
            } finally {
                try {
                    b = lineReader.readLine(currentLine);
                } catch (IOException ignored) {
                }
            }
        }
        return eof = true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(date);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
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
            return Math.min(1.0f, (compressionInputStream.getPos() - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}
