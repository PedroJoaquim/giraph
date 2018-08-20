package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.GiraphInputFormat;
import org.apache.giraph.io.InputType;
import org.apache.giraph.io.checkpoint.CheckpointInputFormat;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.InputSplitsCallable;
import org.apache.giraph.worker.WorkerInputSplitsHandler;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class CheckpointLoaderCallable
        <I extends WritableComparable,
                V extends Writable,
                E extends Writable,
                M extends Writable>
        extends InputSplitsCallable<I, V, E> {

    private final BspServiceWorker<I, V, E> serviceWorker;

    private final VertexCheckpointWriter<I, V, E, M> vertexCheckpointWriter;

    private final CheckpointInputFormat<I, E> checkpointInputFormat;

    private final InputType inputType;

    public static long localVerticesLoaded = 0;
    public static long remoteVerticesLoaded = 0;

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(CheckpointLoaderCallable.class);

    private long[] verticesLoadedPerWorker;

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
                                    VertexCheckpointWriter<I, V, E, M> vertexCheckpointWriter,
                                    CheckpointInputFormat<I, E> checkpointInputFormat,
                                    InputType inputType) {

        super(context, configuration, bspServiceWorker, splitsHandler);

        this.verticesLoadedPerWorker = new long[getMaxWorkerTaskId(bspServiceWorker) + 1];
        this.inputType = inputType;
        this.serviceWorker = bspServiceWorker;
        this.vertexCheckpointWriter = vertexCheckpointWriter;
        this.checkpointInputFormat = checkpointInputFormat;
    }

    private int getMaxWorkerTaskId(BspServiceWorker<I, V, E> worker) {

        int max = Integer.MIN_VALUE;

        for (PartitionOwner po : worker.getPartitionOwners()) {
            if(po.getWorkerInfo().getTaskId() > max){
                max = po.getWorkerInfo().getTaskId();
            }
        }

        return max;
    }


    @Override
    public GiraphInputFormat getInputFormat() {
        return this.checkpointInputFormat;
    }

    @Override
    public InputType getInputType() {
        return this.inputType;
    }

    @Override
    protected VertexEdgeCount readInputSplit(InputSplit inputSplit) throws IOException, InterruptedException {

        LineRecordReader recordReader = new LineRecordReader();

        recordReader.initialize(inputSplit, this.serviceWorker.getContext());

        VertexEdgeCount result;

        if(this.inputType == InputType.CHECKPOINT_VERTICES){
            result = loadVerticesCheckpointSplit(recordReader);
        }
        else if(this.inputType == InputType.CHECKPOINT_MESSAGES){
            result = loadMessagesCheckpointSplit(recordReader);
        }
        else {
            throw new RuntimeException("UNKNOWN INPUT TYPE FOR CHECKPOINT LOADER CALLABLE");
        }

        return result;
    }

    private VertexEdgeCount loadVerticesCheckpointSplit(LineRecordReader recordReader) throws IOException {

        int vertexCount = 0;

        while (recordReader.nextKeyValue()){

            Text line = recordReader.getCurrentValue();

            Vertex<I, V, E> vertex = this.vertexCheckpointWriter.readVertex(line.toString());

            PartitionOwner po = this.serviceWorker.getVertexPartitionOwner(vertex.getId());

            workerClientRequestProcessor.sendVertexRequest(
                    po,
                    vertex);

            vertexCount++;

            verticesLoadedPerWorker[po.getWorkerInfo().getTaskId()]++;
        }

        recordReader.close();

        int myId = serviceWorker.getWorkerInfo().getTaskId();

        LOG.info("debug-load:" + myId + ":" + Arrays.toString(verticesLoadedPerWorker));

        int totalRemoteVertices = 0;
        int totalLocalVertices = 0;

        for (int i = 0; i < verticesLoadedPerWorker.length; i++) {
            if(i != myId){
                totalRemoteVertices += verticesLoadedPerWorker[i];
            }
            else {
                totalLocalVertices += verticesLoadedPerWorker[i];
            }
        }

        addVerticesLoaded(totalRemoteVertices, totalLocalVertices);

        return new VertexEdgeCount(vertexCount, 0, 0);

    }

    private VertexEdgeCount loadMessagesCheckpointSplit(LineRecordReader recordReader) throws IOException {

        int messageCount = 0;

        while (recordReader.nextKeyValue()){

            Text line = recordReader.getCurrentValue();

            VertexCheckpointWriter<I, V, E, M>.MessagesInfo<I, M> msgsInfo
                    = this.vertexCheckpointWriter.readMessagesAndTarget(line.toString());

            I targetId = msgsInfo.getVertexId();

            for (M message : msgsInfo.getMessages()) {
                workerClientRequestProcessor.sendMessageRequest(targetId, message);
                messageCount++;
            }

        }

        recordReader.close();

        return new VertexEdgeCount(messageCount, 0, 0);
    }

    private synchronized void addVerticesLoaded(long remoteVerticesLoaded, long localVerticesLoaded){
        CheckpointLoaderCallable.remoteVerticesLoaded += remoteVerticesLoaded;
        CheckpointLoaderCallable.localVerticesLoaded += localVerticesLoaded;
    }
}
