package main;

import input.CombineArchivesInputFormat;
import mapreduce.ParserMapper;
import mapreduce.ParserReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 11/7/15.
 */
public class TwitterParser {

    public static final String INPUT_PATH = "/testdata";
    public static final String OUTPUT_PATH = "/parser/output";
    public static final String REGEX = "/#flu|[F|f]lu|[I|i]nfluenza|[H|h][1|3|7][N|n][1|3|9]/g\n";
    public static final String TIME_ZONE = "Pacific Time (US & Canada)";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);

        Configuration configuration = new Configuration();

        Job parserJob = Job.getInstance(configuration, "TwitterParser");

        parserJob.setJarByClass(TwitterParser.class);

        //set the combine file size to maximum 64MB
        parserJob.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));
        parserJob.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);

        parserJob.setMapperClass(ParserMapper.class);
        parserJob.setCombinerClass(ParserReducer.class);
        parserJob.setReducerClass(ParserReducer.class);

        FileInputFormat.setInputPaths(parserJob, inputPath);
        FileInputFormat.setInputDirRecursive(parserJob, true);
        parserJob.setInputFormatClass(CombineArchivesInputFormat.class);

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }
        FileOutputFormat.setOutputPath(parserJob, outputPath);
        parserJob.setMapOutputKeyClass(Text.class);
        parserJob.setMapOutputValueClass(IntWritable.class);
        parserJob.setOutputFormatClass(TextOutputFormat.class);
        parserJob.setOutputKeyClass(Text.class);
        parserJob.setOutputValueClass(IntWritable.class);

        parserJob.waitForCompletion(true);
    }

}
