package org.apache.giraph.io.internal;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.checkpoint.CheckpointInputFormat;
import org.apache.giraph.job.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * For internal use only.
 *
 * Wraps user set {@link CheckpointInputFormat} to make sure proper configuration
 * parameters are passed around, that user can set parameters in
 * configuration and they will be available in other methods related to this
 * format.
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public class WrappedCheckpointInputFormat
        <I extends WritableComparable,
        E extends Writable> extends CheckpointInputFormat<I, E> {

    /** {@link CheckpointInputFormat} which is wrapped */
    private final CheckpointInputFormat<I, E> originalInputFormat;

    /**
     * Constructor
     *
     * @param checkpointDir
     * @param checkpointInputFormat Edge input format to wrap
     */
    public WrappedCheckpointInputFormat(String checkpointDir, CheckpointInputFormat<I, E> checkpointInputFormat) {
        super(checkpointDir);
        this.originalInputFormat = checkpointInputFormat;
    }

    @Override
    public void checkInputSpecs(Configuration conf) {
        originalInputFormat.checkInputSpecs(getConf());
    }

    @Override
    public List<InputSplit> getSplits(JobContext context,
                                      int minSplitCountHint) throws IOException, InterruptedException {
        return originalInputFormat.getSplits(
                HadoopUtils.makeJobContext(getConf(), context),
                minSplitCountHint);
    }


    @Override
    public void writeInputSplit(InputSplit inputSplit,
                                DataOutput dataOutput) throws IOException {
        originalInputFormat.writeInputSplit(inputSplit, dataOutput);
    }

    @Override
    public InputSplit readInputSplit(
            DataInput dataInput) throws IOException, ClassNotFoundException {
        return originalInputFormat.readInputSplit(dataInput);
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
        super.setConf(conf);
        this.originalInputFormat.setConf(conf);
    }
}
