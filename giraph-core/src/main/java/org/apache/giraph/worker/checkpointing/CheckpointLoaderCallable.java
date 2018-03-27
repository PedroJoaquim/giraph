package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.InputType;
import org.apache.giraph.io.checkpoint.CheckpointInputFormat;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.InputSplitsCallable;
import org.apache.giraph.worker.WorkerInputSplitsHandler;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointHandler;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class CheckpointLoaderCallable
        <I extends WritableComparable,
        V extends Writable, E extends Writable>
        extends InputSplitsCallable<I, V, E> {

    private final BspServiceWorker<I, V, E> serviceWorker;

    private final VertexCheckpointHandler<I, V, E> vertexCheckpointWriter;

    private long superstep;

    /**
     * Constructor.
     *  @param context          Context
     * @param configuration    Configuration
     * @param bspServiceWorker service worker
     * @param splitsHandler    Handler for input splits
     * @param vertexCheckpointWriter vertex info reader from checkpoint
     */
    public CheckpointLoaderCallable(long superstep,
                                    Mapper<?, ?, ?, ?>.Context context,
                                    ImmutableClassesGiraphConfiguration<I, V, E> configuration,
                                    BspServiceWorker<I, V, E> bspServiceWorker,
                                    WorkerInputSplitsHandler splitsHandler,
                                    VertexCheckpointHandler<I, V, E> vertexCheckpointWriter) {

        super(context, configuration, bspServiceWorker, splitsHandler);

        this.serviceWorker = bspServiceWorker;
        this.vertexCheckpointWriter = vertexCheckpointWriter;
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

        LineRecordReader recordReader = new LineRecordReader();

        recordReader.initialize(inputSplit, this.serviceWorker.getContext());

        int vertexCount = 0;

        while (recordReader.nextKeyValue()){

            Text line = recordReader.getCurrentValue();

            Vertex<I, V, E> vertex = this.vertexCheckpointWriter.readVertex(line.toString());

            workerClientRequestProcessor.addVertexRequest(vertex);

            vertexCount++;
        }

        recordReader.close();

        return new VertexEdgeCount(vertexCount, 0, 0);
    }
}
