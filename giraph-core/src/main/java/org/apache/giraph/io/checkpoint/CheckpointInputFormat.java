package org.apache.giraph.io.checkpoint;

import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.formats.GiraphTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

public class CheckpointInputFormat<I extends WritableComparable,
        E extends Writable> extends GiraphInputFormat<I, Writable, E> {

    private String checkpointDir;

    protected GiraphTextInputFormat textInputFormat = new GiraphTextInputFormat();

    public CheckpointInputFormat(String checkpointDir) {
        this.checkpointDir = checkpointDir;
    }

    @Override
    public void checkInputSpecs(Configuration conf) {

    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException, InterruptedException {
        return textInputFormat.getCheckpointSplits(context, checkpointDir);
    }
}
