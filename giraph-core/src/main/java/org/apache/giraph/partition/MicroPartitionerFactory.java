package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import java.util.*;

public class MicroPartitionerFactory<V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(MicroPartitionerFactory.class);

    private int[] workerInitialMicroPartitionAssignment;

    private boolean metisPartitioningDone;

    private int numMicroPartitions;

    private int[] microPartitionToWorkerMapping;

    private int numPartitionsPerWorker;

    private int numWorkers;

    private int numMicroPartitionsPerWorker;

    public MicroPartitionerFactory() {
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

    public int getMicroPartition(LongWritable id){
        return Math.abs(id.hashCode()) % this.numMicroPartitions;
    }

    @Override
    public int getPartition(LongWritable id, int partitionCount, int workerCount) {

        if(metisPartitioningDone){
            int microPartitionID = getMicroPartition(id);

            int assignedWorker = this.microPartitionToWorkerMapping[microPartitionID];

            int newPartitionId = microPartitionID % this.numPartitionsPerWorker;

            int basePartitionForWorker = assignedWorker * this.numPartitionsPerWorker;

            return basePartitionForWorker + newPartitionId;
        }
        else {
            int microPartitionId = getMicroPartition(id);

            int workerIdx = Math.min(numWorkers - 1, microPartitionId/this.numMicroPartitionsPerWorker);

            return (microPartitionId % numPartitionsPerWorker) + (workerIdx * numPartitionsPerWorker);
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

    public int[] getWorkerInitialMicroPartitionAssignment() {
        return workerInitialMicroPartitionAssignment;
    }

    public boolean isMetisPartitioningDone() {
        return metisPartitioningDone;
    }

    public int getNumMicroPartitions() {
        return numMicroPartitions;
    }

    public int[] getMicroPartitionToWorkerMapping() {
        return microPartitionToWorkerMapping;
    }

    public int getNumPartitionsPerWorker() {
        return numPartitionsPerWorker;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public int getNumMicroPartitionsPerWorker() {
        return numMicroPartitionsPerWorker;
    }
}
