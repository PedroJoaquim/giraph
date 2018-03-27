package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.WorkerInputSplitsHandler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.concurrent.Callable;

public class CheckpointLoaderCallableFactory<I extends WritableComparable,
        V extends Writable, E extends Writable>
        implements CallableFactory<VertexEdgeCount> {


    /** Mapper context. */
    private final Mapper<?, ?, ?, ?>.Context context;
    /** Configuration. */
    private final ImmutableClassesGiraphConfiguration<I, V, E> configuration;
    /** {@link BspServiceWorker} we're running on. */
    private final BspServiceWorker<I, V, E> bspServiceWorker;
    /** Handler for input splits */
    private final WorkerInputSplitsHandler splitsHandler;
    /** superstep we are restarting from*/
    private long superstep;

    public CheckpointLoaderCallableFactory(long superstep,
                                           Mapper<?, ?, ?, ?>.Context context,
                                           ImmutableClassesGiraphConfiguration<I, V, E> configuration,
                                           BspServiceWorker<I, V, E> bspServiceWorker,
                                           WorkerInputSplitsHandler splitsHandler) {
        this.superstep = superstep;
        this.context = context;
        this.configuration = configuration;
        this.bspServiceWorker = bspServiceWorker;
        this.splitsHandler = splitsHandler;
    }

    @Override
    public Callable<VertexEdgeCount> newCallable(int callableId) {
        return new CheckpointLoaderCallable<I, V, E>(
                superstep,
                context,
                configuration,
                bspServiceWorker,
                splitsHandler);
    }
}
