package org.apache.giraph.partition;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MicroPartitionerFactory<V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(MicroPartitionerFactory.class);

    private int[] workerInitialMicroPartitionAssignment;

    private Long2IntMap vertexToPartitionMapping;

    private boolean greedyMetisPartitioning;

    private boolean metisPartitioningDone;

    private int numMicroPartitions;

    private int[] microPartitionToWorkerMapping;

    private int numPartitionsPerWorker;

    private int numWorkers;

    private int numMicroPartitionsPerWorker;

    public MicroPartitionerFactory() {
        this.greedyMetisPartitioning = false;
        this.metisPartitioningDone = false;
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, V, E> conf) {
        super.setConf(conf);

        if(conf.isMETISPartitioning()){
            this.numMicroPartitions = conf.getUserPartitionCount();
            this.numPartitionsPerWorker = conf.getNumComputeThreads();
            this.numWorkers = conf.getMaxWorkers();
            this.numMicroPartitionsPerWorker = this.numMicroPartitions / this.numWorkers;

            this.workerInitialMicroPartitionAssignment = new int[numWorkers];

            int numMicroPerWorker = this.numMicroPartitions / this.numWorkers;

            for (int i = 0; i < numWorkers; i++) {
                this.workerInitialMicroPartitionAssignment[i] = i * numMicroPerWorker;
            }
        }
    }

    public int[] getWorkerMicroPartitionIds(int workIndex){

        int initialIdx = workerInitialMicroPartitionAssignment[workIndex];

        int finalIdx = (workIndex == (numWorkers - 1) ?  (numMicroPartitions - 1) : (workerInitialMicroPartitionAssignment[workIndex + 1] - 1));

        return new int[]{initialIdx, finalIdx};
    }

    //micro partitions are no longer assigned to workers with a hashbased function
    public void metisPartitioningDone(int[] microPartitionToWorkerMapping){
        this.microPartitionToWorkerMapping = microPartitionToWorkerMapping;
        this.metisPartitioningDone = true;
    }

    //vertices are no longer assigned to micro partitions with hash based function
    public void greedyPartitioningDone(BspService<LongWritable, V, E> service){

        long start = System.currentTimeMillis();
        readVertexToPartitionMapping(service);
        long end = System.currentTimeMillis();

        this.greedyMetisPartitioning = true;

        LOG.info("debug-metis: time to read vertex mapping from hdfs = " + (end -start)/1000.0d + " secs");
    }

    private void readVertexToPartitionMapping(BspService<LongWritable, V, E> service) {

        final Queue<FileStatus> fsQueue =
                new ConcurrentLinkedQueue<>();

        FileStatus[] fileStatuses;

        final Long2IntMap vertexMapping = new Long2IntOpenHashMap();

        final FileSystem fs = service.getFs();

        try {
            fileStatuses = fs.listStatus(new Path(service.getConfiguration().getVertexAssignmentPath()));
            fsQueue.addAll(Arrays.asList(fileStatuses));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(service.getConfiguration()),
                fileStatuses.length);


        CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
            @Override
            public Callable<Void> newCallable(int callableId) {
                return new Callable<Void>() {
                    @Override
                    public Void call() throws IOException {

                        while (!fsQueue.isEmpty()) {

                            FileStatus targetFile = fsQueue.poll();

                            if (targetFile == null) {
                                break;
                            }

                            Path targetPartitionInfoPath = targetFile.getPath();

                            if (!targetPartitionInfoPath.getName().endsWith(".mapping")) {
                                continue;
                            }

                            FSDataInputStream fileStream =
                                    fs.open(targetPartitionInfoPath);

                            int partitionId = fileStream.readInt();

                            long numVertices = fileStream.readLong();

                            long[] ids = new long[(int)numVertices];

                            for (int i = 0; i < numVertices; i++) {
                                ids[i] = fileStream.readLong();
                            }

                            fileStream.close();

                            synchronized (vertexMapping){
                                for (int i = 0; i < ids.length; i++) {
                                    vertexMapping.put(ids[i], partitionId);
                                }
                            }
                        }

                        return null;
                    }
                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "metis-read-%d", service.getContext());

        this.vertexToPartitionMapping = vertexMapping;
    }

    public int getMicroPartition(LongWritable id){
        return Math.abs(id.hashCode()) % this.numMicroPartitions;
    }

    @Override
    public int getPartition(LongWritable id, int partitionCount, int workerCount) {

        if(!getConf().isMETISPartitioning()){
            return Math.abs(id.hashCode() % partitionCount);
        }

        if(!greedyMetisPartitioning && !metisPartitioningDone){

            int microPartitionId = Math.abs(id.hashCode()) % this.numMicroPartitions;

            int workerIdx = Math.min(numWorkers - 1, microPartitionId/this.numMicroPartitionsPerWorker);

            return (microPartitionId % numPartitionsPerWorker) + (workerIdx * numPartitionsPerWorker);
        }
        else if(greedyMetisPartitioning){
            return this.vertexToPartitionMapping.get(id.get());
        }
        else{
            int microPartitionID = Math.abs(id.hashCode() % this.numMicroPartitions);

            int assignedWorker = this.microPartitionToWorkerMapping[microPartitionID];

            int newPartitionId = Math.abs(microPartitionID % this.numPartitionsPerWorker);

            int basePartitionForWorker = assignedWorker * this.numPartitionsPerWorker;

            return basePartitionForWorker + newPartitionId;
        }
    }

    //give workers contiguous partitions
    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return partition % workerCount;
    }

    @Override
    public final MasterGraphPartitioner<LongWritable, V, E> createMasterGraphPartitioner() {
        return new MasterGraphPartitionerImpl<LongWritable, V, E>(getConf()) {

            @Override
            protected int getWorkerIndex(int partition, int partitionCount,
                                         int workerCount) {
                return MicroPartitionerFactory.this.getWorker(
                        partition, partitionCount, workerCount);
            }

            @Override
            public Collection<PartitionOwner> createInitialPartitionOwners(
                    Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {

                if(!getConf().isMETISPartitioning() || !getConf().isUndirectedGraph()){
                    return super.createInitialPartitionOwners(availableWorkerInfos, maxWorkers);
                }
                else{
                    int numPartitionsPerWorker = MicroPartitionerFactory.this.numPartitionsPerWorker;

                    int numWorkers = MicroPartitionerFactory.this.numWorkers;

                    List<PartitionOwner> initialPartitionOwners = new ArrayList<>(numWorkers * numPartitionsPerWorker);

                    for (WorkerInfo workerInfo : availableWorkerInfos) {

                        int workerTaskIdx = workerInfo.getWorkerIndex();

                        for (int i = 0; i < numPartitionsPerWorker; i++) {

                            int partitionId = (workerTaskIdx * numPartitionsPerWorker) + i;

                            initialPartitionOwners.add(new BasicPartitionOwner(partitionId, workerInfo));
                        }
                    }

                    this.setPartitionOwners(initialPartitionOwners);

                    return initialPartitionOwners;
                }
            }
        };
    }

}
