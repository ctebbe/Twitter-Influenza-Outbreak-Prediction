package input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Qiu on 11/25/2015.
 */
public class CombineArchivesReader extends RecordReader<Text, Text>{

    private int index;
    private ArchiveReader archiveReader;

    public CombineArchivesReader(CombineFileSplit split, TaskAttemptContext context, Integer index) {
        super();
        this.index = index;
        this.archiveReader = new ArchiveReader(context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(index),
                combineFileSplit.getOffset(index), combineFileSplit.getLength(), combineFileSplit.getLocations());

        archiveReader.initialize(fileSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return archiveReader.nextKeyValue();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return archiveReader.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return archiveReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return archiveReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        archiveReader.close();
    }
}
