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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class OfflineMetisPartitionerFactory<V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(OfflineMetisPartitionerFactory.class);

    private int[] finalPartitionToWorkerMapping;

    private int[] vertexToPartitionMapping;

    private int vertexMappingBegin;

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, V, E> conf) {
        super.setConf(conf);

        this.vertexMappingBegin = conf.getVertexMappingBegin();

        int numWorkers = conf.getMaxWorkers();

        String partitionMappingFileName = getConf().getMicroPartitionMappingPath();

        boolean baseMetis = partitionMappingFileName.isEmpty();

        int numGraphVertices = getConf().getNumGraphVertices();

        this.vertexToPartitionMapping = readMappingToArray(numGraphVertices, getConf().getVertexMappingPath());

        int numPartitionsPerWorker = conf.getNumComputeThreads() * 2;

        this.finalPartitionToWorkerMapping = new int[numWorkers * numPartitionsPerWorker];

        for (int workerIdx = 0; workerIdx < numWorkers; workerIdx++) {
            for (int partitionId = 0; partitionId < numPartitionsPerWorker; partitionId++) {

                int finalPartitionId = (workerIdx * numPartitionsPerWorker) + partitionId;

                this.finalPartitionToWorkerMapping[finalPartitionId] = workerIdx;
            }
        }

        if (baseMetis){
            LOG.info("debug-metis: running base METIS partitioning");

            for (int i = 0; i < this.vertexToPartitionMapping.length; i++) {

                int vertexId = i + this.vertexMappingBegin;

                int metisPartitionID = this.vertexToPartitionMapping[i];

                int newLocalPartitionId = (vertexId % numPartitionsPerWorker);

                int basePartitionForWorker = metisPartitionID * numPartitionsPerWorker;

                this.vertexToPartitionMapping[i] = basePartitionForWorker + newLocalPartitionId;
            }

        }
        else {
            LOG.info("debug-metis: running MICRO-METIS partitioning");

            int[] microPartitionToWorkerMapping = readMappingToList(partitionMappingFileName);

            for (int i = 0; i < this.vertexToPartitionMapping.length; i++) {

                int vertexId = i + this.vertexMappingBegin;

                int metisMicroPartitionID = this.vertexToPartitionMapping[i];

                int workerId = microPartitionToWorkerMapping[metisMicroPartitionID];

                int newLocalPartitionId = (vertexId % numPartitionsPerWorker);

                int basePartitionForWorker = workerId * numPartitionsPerWorker;

                this.vertexToPartitionMapping[i] = basePartitionForWorker + newLocalPartitionId;
            }

        }

        LOG.info("debug-metis: FINISHED METIS PARTITIONING");

    }

    private int[] readMappingToList(String fileHDFSPath) {

        List<Integer> mapping = new ArrayList<>();

        try {

            Path path = new Path(fileHDFSPath);

            FileSystem fs = FileSystem.get(getConf());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            try {
                String line;

                while ((line = br.readLine()) != null){
                    mapping.add(Integer.parseInt(line));
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

        int[] arrayMapping =  new int[mapping.size()];

        for (int i = 0; i < mapping.size(); i++) {
            arrayMapping[i] = mapping.get(i);
        }

        return arrayMapping;
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
        return this.vertexToPartitionMapping[(int) (id.get() - this.vertexMappingBegin)];
    }

    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return finalPartitionToWorkerMapping[partition];
    }
}
