package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OfflineMetisPartitionerFactory <V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(OfflineMetisPartitionerFactory.class);

    private int[] partitionToWorkerMapping;

    private int[] vertexToPartitionMapping;

    private int numPartitionsPerWorker;

    private int vertexMappingBegin;

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, V, E> conf) {
        super.setConf(conf);

        this.numPartitionsPerWorker = conf.getNumComputeThreads();

        this.vertexMappingBegin = conf.getVertexMappingBegin();

        int numUserPartitions = conf.getUserPartitionCount();

        int numWorkers = conf.getMaxWorkers();

        if(numUserPartitions < numWorkers){
            throw new RuntimeException("debug-metis: NUM PARTITIONS IS LESS THAN THE NUMBER OF WORKERS");
        }
        else if (numWorkers == numUserPartitions){
            LOG.info("debug-metis: running base METIS partitioning");

            this.partitionToWorkerMapping = new int[numWorkers];

            for (int i = 0; i < numWorkers; i++) {
                this.partitionToWorkerMapping[i] = i;
            }
        }
        else {

            if(getConf().isRandomMicroAssignment()){
                LOG.info("debug-metis: running random micro METIS partitioning");
            }
            else {
                LOG.info("debug-metis: running micro METIS partitioning");
            }

            this.partitionToWorkerMapping = readMappingToArray(numUserPartitions, getConf().getMicroPartitionMappingPath());
        }

        int numGraphVertices = getConf().getNumGraphVertices();

        this.vertexToPartitionMapping = readMappingToArray(numGraphVertices, getConf().getVertexMappingPath());
    }

    private int[] readMappingToArray(int arraySize, String fileHDFSPath) {

        int[] arrayMapping = new int[arraySize];

        int lineNumber = 0;

        try {

            Path path = new Path(fileHDFSPath);

            FileSystem fs = FileSystem.get(getConf());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            try {
                String line;

                while ((line = br.readLine()) != null){
                    arrayMapping[lineNumber++] = Integer.parseInt(line);
                }

            } finally {
                br.close();
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return arrayMapping;
    }

    @Override
    public int getPartition(LongWritable id, int partitionCount, int workerCount) {

        int initialPartitionId = this.vertexToPartitionMapping[(int) (id.get() - this.vertexMappingBegin)];

        int assignedWorker = this.partitionToWorkerMapping[initialPartitionId];

        int newLocalPartitionId = ((int) id.get() % this.numPartitionsPerWorker);

        int basePartitionForWorker = assignedWorker * this.numPartitionsPerWorker;

        return basePartitionForWorker + newLocalPartitionId;
    }

    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        throw new NotImplementedException(); //should not be called in this class
    }

    @Override
    public final MasterGraphPartitioner<LongWritable, V, E> createMasterGraphPartitioner() {
        return new MasterGraphPartitionerImpl<LongWritable, V, E>(getConf()) {

            @Override
            protected int getWorkerIndex(int partition, int partitionCount,
                                         int workerCount) {
                return OfflineMetisPartitionerFactory.this.getWorker(
                        partition, partitionCount, workerCount);
            }

            @Override
            public Collection<PartitionOwner> createInitialPartitionOwners(
                    Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {

                int numPartitionsPerWorker = getConf().getNumComputeThreads();

                int numWorkers = getConf().getMaxWorkers();

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
        };
    }
}
