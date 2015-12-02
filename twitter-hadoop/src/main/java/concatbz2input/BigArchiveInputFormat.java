package concatbz2input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Qiu on 12/1/2015.
 */
public class BigArchiveInputFormat extends FileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new BigArchiveReader((FileSplit) split, context);
//        return new ArchiveReader(context);
    }

}
