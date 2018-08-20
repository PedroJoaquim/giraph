package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.checkpointing.WorkerCheckpointHandler;
import org.apache.giraph.comm.messages.primitives.IdByteArrayMessageStore;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.utils.*;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DefaultWorkerCheckpointHandler
        <I extends WritableComparable,
        V extends Writable,
        E extends Writable> extends WorkerCheckpointHandler<I, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(DefaultWorkerCheckpointHandler.class);

    private boolean resetPartitioning;

    private List<Integer> myPartitionIds;

    private List<Integer> previousPartitionsIds;

    @Override
    public VertexEdgeCount loadCheckpoint(long superstep) {

        this.myPartitionIds = new ArrayList<>();
        this.previousPartitionsIds = new ArrayList<>();

        long startCheckpointRestart = System.currentTimeMillis();

        int previousNumberOfPartitions = readPreviousNumberOfPartitions(superstep);
        int newNumberOfPartitions = 0;

        for (PartitionOwner po: getCentralizedServiceWorker().getPartitionOwners()) {
            if(po.getWorkerInfo().getTaskId() == getCentralizedServiceWorker().getWorkerInfo().getTaskId()){
                LOG.info("loadCheckpoint: loading partition " + po.getPartitionId() + " from checkpoint");
                myPartitionIds.add(po.getPartitionId());
            }
            newNumberOfPartitions++;
        }

        LOG.info("debug-checkpoint: previous number of partitions = " + previousNumberOfPartitions
                + " new number of partitions = " + newNumberOfPartitions);

        for (int i = 0; i < previousNumberOfPartitions; i++) {
            previousPartitionsIds.add(i);
        }

        this.resetPartitioning = newNumberOfPartitions != previousNumberOfPartitions;

        LOG.info("debug-checkpoint: Loading checkpoint as " + (resetPartitioning ? "RESET" : "MICRO"));

        try {
            GlobalStats globalStats = loadVerticesAndMessagesFromHDFS(superstep);

            loadWorkerContextFromCheckpoint(superstep);

            long endCheckpointRestart = System.currentTimeMillis();

            writeRestartFromCheckpointTime((endCheckpointRestart - startCheckpointRestart) / 1000.0);

            return new VertexEdgeCount(globalStats.getVertexCount(),
                    globalStats.getEdgeCount(), 0);
        } catch (IOException e) {
            throw new RuntimeException(
                    "loadCheckpoint: Failed for superstep=" + superstep, e);
        }
    }

    private void loadWorkerContextFromCheckpoint(long superstep) throws IOException {

        String metadataCheckpointPath =
                getPathManager().createMetadataCheckpointFilePath(superstep, true, 0); //assume all the same

        DataInputStream metadataStream =
                getBspService().getFs().open(new Path(metadataCheckpointPath));

        int previousNumberOfPartitions = metadataStream.readInt();

        for (int i = 0; i < previousNumberOfPartitions; i++) {
            metadataStream.readInt();
        }

        getCentralizedServiceWorker().getWorkerContext().readFields(metadataStream);

        metadataStream.close();

    }

    protected GlobalStats loadVerticesAndMessagesFromHDFS(long superstep) throws IOException {

        String finalizedCheckpointPath =
                getPathManager().createFinalizedCheckpointFilePath(superstep, true);

        GlobalStats globalStats = new GlobalStats();

        loadVerticesFromHDFS(superstep);

        getContext().progress();

        // Load global stats and superstep classes

        SuperstepClasses superstepClasses = SuperstepClasses.createToRead(
                getConfig());

        DataInputStream finalizedStream =
                getBspService().getFs().open(new Path(finalizedCheckpointPath));
        globalStats.readFields(finalizedStream);
        superstepClasses.readFields(finalizedStream);
        getConfig().updateSuperstepClasses(superstepClasses);
        getCentralizedServiceWorker().getServerData().resetMessageStores();

        loadMessagesFromHDFS(superstep);

        // Communication service needs to setup the connections prior to
        // processing vertices
/*if[HADOOP_NON_SECURE]
      workerClient.setup();
else[HADOOP_NON_SECURE]*/
        getCentralizedServiceWorker().getWorkerClient()
                .setup(getConfig().authenticate());
        /*end[HADOOP_NON_SECURE]*/

        return globalStats;
    }

    private void loadMessagesFromHDFS(long superstep) {
        if(resetPartitioning){
            loadCheckpointMessagesReset(superstep, previousPartitionsIds, myPartitionIds);
        }
        else {
            loadCheckpointMessages(superstep, myPartitionIds);
        }
    }

    private void loadVerticesFromHDFS(long superstep) {
        if(resetPartitioning){
            loadCheckpointVerticesReset(superstep, previousPartitionsIds, myPartitionIds);
        }
        else {
            loadCheckpointVertices(superstep, myPartitionIds);
        }
    }

    /**
     * Load saved partitions in multiple threads.
     * @param superstep superstep to load
     * @param partitions list of partitions to load
     */
    private void loadCheckpointVertices(final long superstep,
                                        List<Integer> partitions) {

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfig()),
                partitions.size());

        final Queue<Integer> partitionIdQueue =
                new ConcurrentLinkedQueue<>(partitions);

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
                        while (!partitionIdQueue.isEmpty()) {
                            Integer partitionId = partitionIdQueue.poll();
                            if (partitionId == null) {
                                break;
                            }

                            Path verticesPath =
                                    new Path(getPathManager().
                                            createVerticesCheckpointFilePath(superstep, true, partitionId));

                            FSDataInputStream verticesUncompressedStream =
                                    getBspService().getFs().open(verticesPath);


                            DataInputStream verticesStream = codec == null ? verticesUncompressedStream :
                                    new DataInputStream(
                                            codec.createInputStream(verticesUncompressedStream));


                            Partition<I, V, E> partition =
                                    getConfig().createPartition(partitionId, getContext());

                            partition.readFields(verticesStream);

                            getCentralizedServiceWorker().getPartitionStore().addPartition(partition);

                            verticesStream.close();
                            verticesUncompressedStream.close();

                        }
                        return null;
                    }

                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "load-vertices-%d", getContext());

        LOG.info("Loaded checkpoint in " + (System.currentTimeMillis() - t0) +
                " ms, using " + numThreads + " threads");
    }


    private void loadCheckpointVerticesReset(final long superstep,
                                             final List<Integer> previousPartitionsIds,
                                             final List<Integer> myPartitionIds) {

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfig()),
                previousPartitionsIds.size());

        final Queue<Integer> partitionIdQueue =
                new ConcurrentLinkedQueue<>(previousPartitionsIds);

        final CompressionCodec codec =
                new CompressionCodecFactory(getConfig())
                        .getCodec(new Path(
                                GiraphConstants.CHECKPOINT_COMPRESSION_CODEC
                                        .get(getConfig())));

        long t0 = System.currentTimeMillis();

        final HashMap<Integer, Partition<I, V, E>> myPartitions = new HashMap<>();

        for (Integer id: myPartitionIds) {
            myPartitions.put(id, getConfig().createPartition(id, getContext()));
        }

        CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
            @Override
            public Callable<Void> newCallable(int callableId) {
                return new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {

                        HashMap<Integer, Partition<I, V, E>> threadLocalPartitions = new HashMap<>();

                        for (Integer id: myPartitionIds) {
                            threadLocalPartitions.put(id, getConfig().createPartition(id, getContext()));
                        }

                        while (!partitionIdQueue.isEmpty()) {

                            Integer partitionId = partitionIdQueue.poll();

                            if (partitionId == null) {
                                break;
                            }

                            Path verticesPath =
                                    new Path(getPathManager().
                                            createVerticesCheckpointFilePath(superstep, true, partitionId));


                            FSDataInputStream verticesUncompressedStream =
                                    getBspService().getFs().open(verticesPath);


                            DataInputStream verticesStream = codec == null ? verticesUncompressedStream :
                                    new DataInputStream(
                                            codec.createInputStream(verticesUncompressedStream));


                            //TODO: improve this mess

                            //ignore id
                            verticesStream.readInt();

                            //num vertices
                            int vertices = verticesStream.readInt();

                            for (int i = 0; i < vertices; ++i) {
                                Vertex<I, V, E> vertex =
                                        WritableUtils.readVertexFromDataInput(verticesStream,
                                                getConfig());

                                PartitionOwner partitionOwner = getCentralizedServiceWorker()
                                        .getVertexPartitionOwner(vertex.getId());


                                if(partitionOwner.getWorkerInfo().getTaskId() ==
                                        getCentralizedServiceWorker().getWorkerInfo().getTaskId()){
                                    threadLocalPartitions.get(partitionOwner.getPartitionId()).putVertex(vertex);
                                }
                            }

                            verticesStream.close();
                            verticesUncompressedStream.close();

                        }

                        for (Integer partitionID: myPartitionIds) {
                            myPartitions.get(partitionID).addPartition(threadLocalPartitions.get(partitionID));
                        }

                        return null;
                    }

                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "load-vertices-%d", getContext());

        for (Partition<I, V, E> p : myPartitions.values()) {
            getCentralizedServiceWorker().getPartitionStore().addPartition(p);
        }

        LOG.info("Loaded checkpoint in " + (System.currentTimeMillis() - t0) +
                " ms, using " + numThreads + " threads");

    }

    private void loadCheckpointMessagesReset(final long superstep,
                                             final List<Integer> previousPartitionsIds,
                                             final List<Integer> myPartitionIds) {

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfig()),
                previousPartitionsIds.size());

        final Queue<Integer> partitionIdQueue =
                new ConcurrentLinkedQueue<>(previousPartitionsIds);

        final CompressionCodec codec =
                new CompressionCodecFactory(getConfig())
                        .getCodec(new Path(
                                GiraphConstants.CHECKPOINT_COMPRESSION_CODEC
                                        .get(getConfig())));

        long t0 = System.currentTimeMillis();

        final HashMap<Integer, Basic2ObjectMap<I, DataInputOutput>> myMessageStores = new HashMap<>();

        for (int myPartitionID : myPartitionIds) {
            myMessageStores.put(myPartitionID, ((IdByteArrayMessageStore<I, Writable>)
                    (getCentralizedServiceWorker().getServerData().getCurrentMessageStore()))
                    .createPartitionObjectMap());
        }

        CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
            @Override
            public Callable<Void> newCallable(int callableId) {
                return new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {
                        while (!partitionIdQueue.isEmpty()) {
                            Integer partitionId = partitionIdQueue.poll();
                            if (partitionId == null) {
                                break;
                            }

                            Basic2ObjectMap<I, DataInputOutput> reader = ((IdByteArrayMessageStore<I, Writable>)
                                    (getCentralizedServiceWorker().getServerData().getCurrentMessageStore()))
                                    .createPartitionObjectMap();

                            Path msgsPath =
                                    new Path(getPathManager().
                                            createMessagesCheckpointFilePath(superstep, true, partitionId));

                            FSDataInputStream msgsUncompressedStream =
                                    getBspService().getFs().open(msgsPath);

                            DataInputStream msgsStream = codec == null ? msgsUncompressedStream :
                                    new DataInputStream(
                                            codec.createInputStream(msgsUncompressedStream));


                            int size = msgsStream.readInt();

                            //TODO: improve this mess
                            while (size-- > 0) {

                                Basic2ObjectMap<I, DataInputOutput>.Pair<I, DataInputOutput> pair =
                                        reader.readNextValueFromInputStream(msgsStream);

                                PartitionOwner partitionOwner =
                                        getCentralizedServiceWorker().getVertexPartitionOwner(pair.getKey());

                                if(partitionOwner.getWorkerInfo().getTaskId() ==
                                        getCentralizedServiceWorker().getWorkerInfo().getTaskId()){
                                    Basic2ObjectMap<I, DataInputOutput> store
                                            = myMessageStores.get(partitionOwner.getPartitionId());

                                    synchronized (store){
                                        store.put(pair.getKey(), pair.getValue());
                                    }
                                }
                            }

                            msgsStream.close();
                            msgsUncompressedStream.close();
                        }
                        return null;
                    }
                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "load-vertices-%d", getContext());

        for (int myPartitionID : myPartitionIds) {
            ((IdByteArrayMessageStore<I, Writable>)
                    (getCentralizedServiceWorker().getServerData().getCurrentMessageStore()))
                    .addMapForPartition(myMessageStores.get(myPartitionID), myPartitionID);
        }

        LOG.info("Loaded checkpoint in " + (System.currentTimeMillis() - t0) +
                " ms, using " + numThreads + " threads");

    }

    /**
     * Load saved partitions in multiple threads.
     * @param superstep superstep to load
     * @param partitions list of partitions to load
     */
    private void loadCheckpointMessages(final long superstep,
                                        List<Integer> partitions) {

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfig()),
                partitions.size());

        final Queue<Integer> partitionIdQueue =
                new ConcurrentLinkedQueue<>(partitions);

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
                        while (!partitionIdQueue.isEmpty()) {
                            Integer partitionId = partitionIdQueue.poll();
                            if (partitionId == null) {
                                break;
                            }

                            Path msgsPath =
                                    new Path(getPathManager().
                                            createMessagesCheckpointFilePath(superstep, true, partitionId));

                            FSDataInputStream msgsUncompressedStream =
                                    getBspService().getFs().open(msgsPath);

                            DataInputStream msgsStream = codec == null ? msgsUncompressedStream :
                                    new DataInputStream(
                                            codec.createInputStream(msgsUncompressedStream));


                            getCentralizedServiceWorker().getServerData().getCurrentMessageStore()
                                    .readFieldsForPartition(msgsStream, partitionId);


                            msgsStream.close();
                            msgsUncompressedStream.close();

                        }
                        return null;
                    }

                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "load-vertices-%d", getContext());

        LOG.info("Loaded checkpoint in " + (System.currentTimeMillis() - t0) +
                " ms, using " + numThreads + " threads");

    }

    /**
     * Save partitions. To speed up this operation
     * runs in multiple threads.
     */

    @Override
    public void storeCheckpoint() throws IOException {

        long superstep = getBspService().getSuperstep();

        int workerId = getCentralizedServiceWorker().getWorkerInfo().getWorkerIndex();

        Path validFilePath = deleteAndCreateCheckpointFilePath(
                getPathManager().createValidCheckpointFilePath(superstep, false, workerId));

        Path metadataFilePath = deleteAndCreateCheckpointFilePath(
                getPathManager().createMetadataCheckpointFilePath(superstep, false, workerId));

        // Metadata is buffered and written at the end since it's small and
        // needs to know how many partitions this worker owns
        FSDataOutputStream metadataOutputStream =
                getBspService().getFs().create(metadataFilePath);
        metadataOutputStream.writeInt(getCentralizedServiceWorker().getPartitionStore().getNumPartitions());

        for (Integer partitionId : getCentralizedServiceWorker().getPartitionStore().getPartitionIds()) {
            metadataOutputStream.writeInt(partitionId);
        }

        getCentralizedServiceWorker().getWorkerContext().write(metadataOutputStream);

        metadataOutputStream.close();

        storeVerticesAndMessages();

        getBspService().getFs().createNewFile(validFilePath);

        // Notify master that checkpoint is stored
        String workerWroteCheckpoint =
                getBspService().getWorkerWroteCheckpointPath(getBspService().getApplicationAttempt(),
                        getBspService().getSuperstep()) + "/" +
                        getCentralizedServiceWorker().getWorkerInfo().getHostnameId();
        try {
            getBspService().getZkExt().createExt(workerWroteCheckpoint,
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("storeCheckpoint: wrote checkpoint worker path " +
                    workerWroteCheckpoint + " already exists!");
        } catch (KeeperException e) {
            throw new IllegalStateException("Creating " + workerWroteCheckpoint +
                    " failed with KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Creating " +
                    workerWroteCheckpoint +
                    " failed with InterruptedException", e);
        }
    }

    protected void storeVerticesAndMessages() {

        final int numPartitions = getCentralizedServiceWorker().getPartitionStore().getNumPartitions();

        final long superstep = getBspService().getSuperstep();

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

                            Path verticesPath = deleteAndCreateCheckpointFilePath(
                                    getPathManager().createVerticesCheckpointFilePath(superstep, false, partition.getId()));

                            Path msgsPath = deleteAndCreateCheckpointFilePath(
                                    getPathManager().createMessagesCheckpointFilePath(superstep, false, partition.getId()));

                            FSDataOutputStream uncompressedVerticesStream =
                                    getBspService().getFs().create(verticesPath);

                            FSDataOutputStream uncompressedMsgsStream =
                                    getBspService().getFs().create(msgsPath);


                            DataOutputStream verticesStream = codec == null ? uncompressedVerticesStream :
                                    new DataOutputStream(
                                            codec.createOutputStream(uncompressedVerticesStream));

                            DataOutputStream msgsStream = codec == null ? uncompressedMsgsStream :
                                    new DataOutputStream(
                                            codec.createOutputStream(uncompressedMsgsStream));

                            partition.write(verticesStream);

                            getCentralizedServiceWorker().getPartitionStore().putPartition(partition);

                            getCentralizedServiceWorker().getServerData().getCurrentMessageStore()
                                    .writePartition(msgsStream, partition.getId());

                            getContext().progress();

                            verticesStream.close();
                            msgsStream.close();

                            uncompressedVerticesStream.close();
                            uncompressedMsgsStream.close();
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



    private int readPreviousNumberOfPartitions(long superstep) {

        int result = 0;

        while (true){
            Path verticesPath = new Path(getPathManager().
                    createVerticesCheckpointFilePath(superstep, true, result));

            try {
                if(getBspService().getFs().exists(verticesPath)){
                    result++;
                }
                else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }

        }

        return result;
    }

    public List<Integer> getMyPartitionIds() {
        return myPartitionIds;
    }

    public List<Integer> getPreviousPartitionsIds() {
        return previousPartitionsIds;
    }
}
