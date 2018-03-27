package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.InputType;
import org.apache.giraph.io.checkpoint.CheckpointInputFormat;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.InputSplitsCallable;
import org.apache.giraph.worker.WorkerInputSplitsHandler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CheckpointLoaderCallable
        <I extends WritableComparable,
        V extends Writable, E extends Writable>
        extends InputSplitsCallable<I, V, E> {

    private long superstep;

    /**
     * Constructor.
     *
     * @param context          Context
     * @param configuration    Configuration
     * @param bspServiceWorker service worker
     * @param splitsHandler    Handler for input splits
     */
    public CheckpointLoaderCallable(long superstep,
                                    Mapper<?, ?, ?, ?>.Context context,
                                    ImmutableClassesGiraphConfiguration<I, V, E> configuration,
                                    BspServiceWorker<I, V, E> bspServiceWorker,
                                    WorkerInputSplitsHandler splitsHandler) {

        super(context, configuration, bspServiceWorker, splitsHandler);
    }


    @Override
    public GiraphInputFormat getInputFormat() {
        return new CheckpointInputFormat();
    }

    @Override
    public InputType getInputType() {
        return InputType.CHECKPOINT;
    }

    @Override
    protected VertexEdgeCount readInputSplit(InputSplit inputSplit) throws IOException, InterruptedException {
        return null; // todo
    }
}
