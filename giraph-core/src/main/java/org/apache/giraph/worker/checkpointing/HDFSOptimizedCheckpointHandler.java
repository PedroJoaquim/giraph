package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointHandler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutputStream;
import java.io.IOException;

public class HDFSOptimizedCheckpointHandler
        <I extends WritableComparable,
                V extends WritableComparable,
                E extends WritableComparable> extends SplitWorkCheckpointHandler<I, V, E> {




    private VertexCheckpointHandler<I, V, E> vertexCheckpointWriter;

    public HDFSOptimizedCheckpointHandler() {
        this.vertexCheckpointWriter = getConfig().createVertexCheckpointHandler(getContext());
    }

    @Override
    protected void writePartitionToStream(DataOutputStream verticesStream, Partition<I, V, E> partition) throws IOException {
        for (Vertex<I, V, E> vertex : partition) {
            this.vertexCheckpointWriter.writeVertex(vertex, verticesStream);
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
                        getCentralizedServiceWorker().getInputSplitsHandler(),
                        vertexCheckpointWriter);


        ProgressableUtils.getResultsWithNCallables(checkpointLoaderCallableFactory,
                numThreads, "load-checkpoint-%d", getContext());


        getCentralizedServiceWorker().getWorkerClient().waitAllRequests();
    }

    @Override
    protected void loadMessagesFromHDFS(long superstep){
        //TODO
    }
}
