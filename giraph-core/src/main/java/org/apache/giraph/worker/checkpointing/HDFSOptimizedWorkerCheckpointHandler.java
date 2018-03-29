package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.checkpointing.CheckpointPathManager;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.InputType;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.concurrent.Callable;


public class HDFSOptimizedWorkerCheckpointHandler
        <I extends WritableComparable,
                V extends Writable,
                E extends Writable,
                M extends Writable> extends DefaultWorkerCheckpointHandler<I, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(HDFSOptimizedWorkerCheckpointHandler.class);

    private VertexCheckpointWriter<I, V, E, M> vertexCheckpointWriter;

    @Override
    public void initialize(CentralizedServiceWorker<I, V, E> centralizedServiceWorker,
                           BspService<I, V, E> bspService,
                           CheckpointPathManager pathManager) {

        super.initialize(centralizedServiceWorker, bspService, pathManager);
        this.vertexCheckpointWriter = getConfig().createVertexCheckpointWriter(getContext());
        this.vertexCheckpointWriter.initialize(getConfig());
    }

    @Override
    protected GlobalStats loadVerticesAndMessagesFromHDFS(long superstep) throws IOException {

        String finalizedCheckpointPath =
                getPathManager().createFinalizedCheckpointFilePath(superstep, true);

        GlobalStats globalStats = new GlobalStats();

        // Load global stats and superstep classes

        SuperstepClasses superstepClasses = SuperstepClasses.createToRead(
                getConfig());

        DataInputStream finalizedStream =
                getBspService().getFs().open(new Path(finalizedCheckpointPath));
        globalStats.readFields(finalizedStream);
        superstepClasses.readFields(finalizedStream);
        getConfig().updateSuperstepClasses(superstepClasses);

        loadVerticesAndMessagesFromHDFSOptimized(superstep);

        getContext().progress();

        // Communication service needs to setup the connections prior to
        // processing vertices
/*if[HADOOP_NON_SECURE]
      workerClient.setup();
else[HADOOP_NON_SECURE]*/
        getCentralizedServiceWorker().getWorkerClient()
                .setup(getConfig().authenticate());
        /*end[HADOOP_NON_SECURE]*/

        getCentralizedServiceWorker().markCurrentWorkerDoneReadingThenWaitForOthers();
        getCentralizedServiceWorker().getServerData().prepareSuperstep();

        return globalStats;
    }

    private void loadVerticesAndMessagesFromHDFSOptimized(long superstep) {

        int numThreads = getConfig().getNumInputSplitsThreads();

        //LOAD VERTICES

        CheckpointLoaderCallableFactory<I, V, E, M> checkpointLoaderCallableFactory =
                new CheckpointLoaderCallableFactory<I, V, E, M>(
                        superstep,
                        getContext(),
                        getConfig(),
                        (BspServiceWorker<I, V, E>) getCentralizedServiceWorker(),
                        getCentralizedServiceWorker().getInputSplitsHandler(),
                        vertexCheckpointWriter,
                        getConfig().createWrappedCheckpointInputFormat(
                                getPathManager().getVerticesCheckpointFilesDirPath(superstep, true)),
                        InputType.CHECKPOINT_VERTICES);

        ProgressableUtils.getResultsWithNCallables(checkpointLoaderCallableFactory,
                numThreads, "load-checkpoint-vertices%d", getContext());


        getCentralizedServiceWorker().getWorkerClient().waitAllRequests();

        //LOAD MESSAGES

        checkpointLoaderCallableFactory =
                new CheckpointLoaderCallableFactory<I, V, E, M>(
                        superstep,
                        getContext(),
                        getConfig(),
                        (BspServiceWorker<I, V, E>) getCentralizedServiceWorker(),
                        getCentralizedServiceWorker().getInputSplitsHandler(),
                        vertexCheckpointWriter,
                        getConfig().createWrappedCheckpointInputFormat(
                                getPathManager().getVerticesCheckpointFilesDirPath(superstep, true)),
                        InputType.CHECKPOINT_MESSAGES);

        ProgressableUtils.getResultsWithNCallables(checkpointLoaderCallableFactory,
                numThreads, "load-checkpoint-messages%d", getContext());


        getCentralizedServiceWorker().getWorkerClient().waitAllRequests();

    }


    @Override
    protected void storeVerticesAndMessages(){

        final int numPartitions = getCentralizedServiceWorker().getPartitionStore().getNumPartitions();

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfig()),
                numPartitions);

        getCentralizedServiceWorker().getPartitionStore().startIteration();

        final CompressionCodec codec =
                new CompressionCodecFactory(getConfig())
                        .getCodec(new Path(
                                GiraphConstants.CHECKPOINT_COMPRESSION_CODEC
                                        .get(getConfig())));


        long t0 = System.currentTimeMillis();

        CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
            @Override
            public Callable<Void> newCallable(int callableId) {
                return new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {
                        while (true) {
                            Partition<I, V, E> partition =
                                    getCentralizedServiceWorker().getPartitionStore().getNextPartition();

                            if (partition == null) {
                                break;
                            }

                            Path verticesPath =
                                    new Path(getPathManager().createVerticesCheckpointFilePath(
                                            getBspService().getSuperstep(),
                                            false,
                                            partition.getId()
                                    ));


                            Path msgsPath =
                                    new Path(getPathManager().createMessagesCheckpointFilePath(
                                            getBspService().getSuperstep(),
                                            false,
                                            partition.getId()
                                    ));


                            MessageStore<I, M> messageStore = getCentralizedServiceWorker().getServerData()
                                    .getCurrentMessageStore();

                            writeVerticesToStream(codec, verticesPath, partition);

                            writeMessagesToStream(codec, msgsPath, messageStore, partition);

                            getCentralizedServiceWorker().getPartitionStore().putPartition(partition);

                            getContext().progress();

                        }
                        return null;
                    }
                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "checkpoint-vertices-%d", getContext());

        LOG.info("Save checkpoint in " + (System.currentTimeMillis() - t0) +
                " ms, using " + numThreads + " threads");

    }

    private void writeMessagesToStream(CompressionCodec codec,
                                       Path msgsPath,
                                       MessageStore<I, M> messageStore,
                                       Partition<I, V, E> partition) throws IOException {


        FSDataOutputStream os =
                getBspService().getFs().create(msgsPath);

        BufferedWriter writer = new BufferedWriter( new OutputStreamWriter(os, "UTF-8" ) );

        for (I vertexId : messageStore.getPartitionDestinationVertices(partition.getId())) {

            this.vertexCheckpointWriter.writeMessagesForVertex(vertexId,
                    messageStore.getVertexMessages(vertexId),
                    writer);
        }

        writer.close();
    }


    private void writeVerticesToStream(CompressionCodec codec,
                                          Path verticesPath,
                                          Partition<I, V, E> partition) throws IOException {

        FSDataOutputStream os =
                getBspService().getFs().create(verticesPath);

        BufferedWriter writer = new BufferedWriter( new OutputStreamWriter(os, "UTF-8" ) );

        for (Vertex<I, V, E> vertex : partition) {
            this.vertexCheckpointWriter.writeVertex(vertex, writer);
        }

        writer.close();
    }

}
