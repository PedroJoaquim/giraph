package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointHandler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class HDFSOptimizedCheckpointHandler
        <I extends WritableComparable,
        V extends WritableComparable,
        E extends WritableComparable> extends SplitWorkCheckpointHandler<I, V, E> {


    private static final String SEPARATOR = "\t";

    private static final String EOL = "\n";

    private final Pattern SEPARATOR_PATTERN = Pattern.compile("[\t ]");

    private VertexCheckpointHandler<I, V, E> vertexCheckpointWriter;

    public HDFSOptimizedCheckpointHandler() {
        this.vertexCheckpointWriter = getConfig().createVertexCheckpointHandler(getContext());
    }

    @Override
    protected void writePartitionToStream(DataOutputStream verticesStream, Partition<I, V, E> partition) throws IOException {

        for (Vertex<I, V, E> vertex : partition) {

            verticesStream.writeUTF(vertexCheckpointWriter.writeVertexID(vertex.getId()));
            verticesStream.writeUTF(SEPARATOR);
            verticesStream.writeUTF(vertexCheckpointWriter.writeVertexValue(vertex.getValue()));

            for (Edge<I, E> edge : vertex.getEdges()) {
                verticesStream.writeUTF(SEPARATOR);
                verticesStream.writeUTF(vertexCheckpointWriter.writeVertexEdge(edge));
            }

            verticesStream.writeUTF(EOL);
        }
    }

    @Override
    protected void writeMessages(DataOutputStream msgsStream,
                                 MessageStore<I, Writable> currentMessageStore,
                                 int partitionId) throws IOException {

        getCentralizedServiceWorker().getServerData().getCurrentMessageStore()
                .writePartition(msgsStream, partitionId);
    }



    @Override
    protected void loadVerticesFromHDFS(long superstep){

        int numThreads = getConfig().getNumInputSplitsThreads();

        CheckpointLoaderCallableFactory<I, V, E> checkpointLoaderCallableFactory =
                new CheckpointLoaderCallableFactory<I, V, E>(
                        superstep,
                        getContext(),
                        getConfig(),
                        (BspServiceWorker<I, V, E>) getCentralizedServiceWorker(),
                        getCentralizedServiceWorker().getInputSplitsHandler());


        List<VertexEdgeCount> results =
                ProgressableUtils.getResultsWithNCallables(checkpointLoaderCallableFactory,
                        numThreads, "load-checkpoint-%d", getContext());


        getCentralizedServiceWorker().getWorkerClient().waitAllRequests();
    }

    @Override
    protected void loadMessagesFromHDFS(long superstep){

    }
}
