package org.apache.giraph.partition;

import org.apache.giraph.checkpointing.CheckpointPathManager;
import org.apache.giraph.master.BspServiceMaster;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class LocalityAwarePartitionerFactory <I extends WritableComparable,
        V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<I, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceMaster.class);

    private int[] partitionAssignment;

    private class PartitionInfo{

        private int id;

        private List<WorkerInfo> workerInfos;

        private double[] scores;

        public PartitionInfo(int id, List<WorkerInfo> workerInfos) {
            this.id = id;
            this.workerInfos = workerInfos;

            this.scores = new double[workerInfos.size()];
        }

        public void addHosts(List<String> verticesHosts, List<String> messagesHosts){

            for (String verticesHost : verticesHosts) {
                for (int i = 0; i < workerInfos.size(); i++) {
                    if(workerInfos.get(i).getHostname().contains(verticesHost)){
                        scores[i] += 1;
                    }
                }
            }

            for (String messagesHost : messagesHosts) {
                for (int i = 0; i < workerInfos.size(); i++) {
                    if(workerInfos.get(i).getHostname().contains(messagesHost)){
                        scores[i] += 0.2;
                    }
                }
            }
        }

        public double getScoreForHost(int workerInfoIdx){
            return this.scores[workerInfoIdx];
        }

        public int getId() {
            return id;
        }

        public double getHighestScore(int iteration) {

            double[] scoresCopy = Arrays.copyOf(scores, scores.length);

            Arrays.sort(scoresCopy);

            return scoresCopy[scoresCopy.length - iteration];
        }
    }

    public void assignPartitionsBasedOnLocality(CheckpointPathManager pathManager,
                                                  long restartSuperstep,
                                                  List<WorkerInfo> workerInfos){

        try {


            FileSystem fs = FileSystem.get(getConf());

            int numPartitions = getConf().getUserPartitionCount();

            this.partitionAssignment = new int[numPartitions];

            List<PartitionInfo> partitionInfos = new ArrayList<>();

            for (int i = 0; i < numPartitions; i++) {

                partitionInfos.add(new PartitionInfo(i, workerInfos));

                Path verticesPath =
                        new Path(pathManager.
                                createVerticesCheckpointFilePath(restartSuperstep, true, i));

                Path messagesPath =
                        new Path(pathManager.
                                createMessagesCheckpointFilePath(restartSuperstep, true, i));


                FileStatus verticesFileStatus = fs.getFileStatus(verticesPath);

                FileStatus messagesFileStatus = fs.getFileStatus(messagesPath);

                BlockLocation[] verticesFileBlockLocatiions = fs.getFileBlockLocations(verticesFileStatus, 0L, verticesFileStatus.getLen());
                BlockLocation[] messagesFileBlockLocatiions = fs.getFileBlockLocations(messagesFileStatus, 0L, messagesFileStatus.getLen());


                List<String> verticesHosts = new ArrayList<>();
                List<String> messagesHosts = new ArrayList<>();



                for (BlockLocation block : verticesFileBlockLocatiions) {
                    verticesHosts.addAll(Arrays.asList(block.getHosts()));
                }

                for (BlockLocation block : messagesFileBlockLocatiions) {
                    messagesHosts.addAll(Arrays.asList(block.getHosts()));
                }

                partitionInfos.get(i).addHosts(verticesHosts, messagesHosts);
            }



            assignPartitionsSimple(workerInfos, numPartitions, partitionInfos);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void assignPartitionsGreedy(List<WorkerInfo> workerInfos, int numPartitions, List<PartitionInfo> partitionInfos) {

        int numAssignedPartitions = 0;

        int[] numPartitionsForHost = new int[workerInfos.size()];

        int partitionsPerHost = numPartitions / workerInfos.size();

        final int iteration = 0;

        while (numAssignedPartitions < numPartitions){

            Collections.sort(partitionInfos, new Comparator<PartitionInfo>() {
                @Override
                public int compare(PartitionInfo o1, PartitionInfo o2) {
                    return (int) ((o2.getHighestScore(iteration) - o1.getHighestScore(iteration)) * 100);
                }
            });


        }
    }
    private void assignPartitionsSimple(List<WorkerInfo> workerInfos, int numPartitions, List<PartitionInfo> partitionInfos) {

        int[] numPartitionsForHost = new int[workerInfos.size()];

        int partitionsPerHost = numPartitions / workerInfos.size();

        if (numPartitions % workerInfos.size() != 0) {
            throw new RuntimeException("[ERROR] INVALID NUMBER OF PARTITIONS FOR AVAILABLE WORKERS");
        }

        for (PartitionInfo partitionInfo : partitionInfos) {

            double maxScore = -1;
            int workerIdx = -1;

            for (int i = 0; i < workerInfos.size(); i++) {

                double score = partitionInfo.getScoreForHost(i);

                if (score > maxScore && numPartitionsForHost[i] < partitionsPerHost) {
                    maxScore = score;
                    workerIdx = i;
                }
            }

            LOG.info("debug-locality: PARTITION " + partitionInfo.getId()
                    + " --> WORKER = " + workerIdx + " | SCORE = " + maxScore);

            numPartitionsForHost[workerIdx]++;
            this.partitionAssignment[partitionInfo.getId()] = workerIdx;
        }
    }



    @Override
    public int getPartition(I id, int partitionCount, int workerCount) {
        return Math.abs(id.hashCode() % partitionCount);
    }

    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return this.partitionAssignment[partition];
    }
}
