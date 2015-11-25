package input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by Qiu on 11/25/2015.
 */
public class CombineArchivesInputFormat extends CombineFileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        CombineFileRecordReader<Text, Text> combineFileRecordReader
                = new CombineFileRecordReader<>(combineFileSplit, context, CombineArchivesReader.class);
        try {
            combineFileRecordReader.initialize(combineFileSplit, context);
        } catch (InterruptedException e) {
            System.err.println("Failed to initialize combine file record reader");
            System.err.println(e.getMessage());
        }
        return combineFileRecordReader;
    }

}
