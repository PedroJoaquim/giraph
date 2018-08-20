package org.apache.giraph.master.checkpointing;

import org.apache.giraph.checkpointing.MasterCheckpointHandler;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.emr.s3.S3Checkpointer;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

public class DefaultMasterCheckpointHandler
        <I extends WritableComparable,
                V extends Writable,
                E extends Writable> extends MasterCheckpointHandler<I, V, E> {


    /** Class logger */
    private static final Logger LOG = Logger.getLogger(DefaultMasterCheckpointHandler.class);

    @Override
    public Collection<PartitionOwner> prepareCheckpointRestart(long superstep) throws IOException {

        List<PartitionOwner> partitionOwners = new ArrayList<>();

        int newNumPartitions = PartitionUtils.computePartitionCount(
               getCentralizedServiceMaster().getWorkerInfoList().size(), getConfig());

        FileSystem fs = getFs();

        String finalizedCheckpointPath =
                getPathManager().createFinalizedCheckpointFilePath(superstep, true);

        LOG.info("Loading checkpoint from " + finalizedCheckpointPath);

        DataInputStream finalizedStream =
                fs.open(new Path(finalizedCheckpointPath));

        GlobalStats globalStats = new GlobalStats();
        globalStats.readFields(finalizedStream);
        updateCounters(globalStats);

        SuperstepClasses superstepClasses =
                SuperstepClasses.createToRead(getConfig());

        superstepClasses.readFields(finalizedStream);

        getConfig().updateSuperstepClasses(superstepClasses);

        int prefixFileCount = finalizedStream.readInt();

        finalizedStream.readUTF(); //backwards compatibility

        int previousNumberOfPartitions = 0;

        for (int i = 0; i < prefixFileCount; ++i) {
            int mrTaskId = finalizedStream.readInt();

            DataInputStream metadataStream = fs.open(new Path(getPathManager().
                    createMetadataCheckpointFilePath(superstep, true, mrTaskId)
            ));

            previousNumberOfPartitions += metadataStream.readInt();

            metadataStream.close();
        }

        LOG.info("debug-checkpoint: previous number of partitions = " + previousNumberOfPartitions);

        int numWorkers = getCentralizedServiceMaster().getWorkerInfoList().size();

        for (int p = 0; p < newNumPartitions; ++p) {

            int workerIndex = getBspService().getGraphPartitionerFactory().getWorker(p, newNumPartitions, numWorkers);
            WorkerInfo worker = getCentralizedServiceMaster().getWorkerInfoList().get(workerIndex);

            PartitionOwner partitionOwner = new BasicPartitionOwner(p, worker);

            partitionOwners.add(partitionOwner);

        }

        //Ordering appears to be important as of right now we rely on this ordering
        //in WorkerGraphPartitioner
        Collections.sort(partitionOwners, new Comparator<PartitionOwner>() {
            @Override
            public int compare(PartitionOwner p1, PartitionOwner p2) {
                return Integer.compare(p1.getPartitionId(), p2.getPartitionId());
            }
        });


        getCentralizedServiceMaster().getGlobalCommHandler().getAggregatorHandler().readFields(finalizedStream);
        getCentralizedServiceMaster().getAggregatorTranslationHandler().readFields(finalizedStream);
        getCentralizedServiceMaster().getMasterCompute().readFields(finalizedStream);
        finalizedStream.close();

        return partitionOwners;
    }

    @Override
    public void finalizeCheckpoint(long superstep, List<WorkerInfo> chosenWorkerInfoList)
            throws IOException, KeeperException, InterruptedException {


        Path finalizedCheckpointPath = new Path(getPathManager().
                createFinalizedCheckpointFilePath(superstep, false));

        try {
            getFs().delete(finalizedCheckpointPath, false);
        } catch (IOException e) {
            LOG.warn("finalizedValidCheckpointPrefixes: Removed old file " +
                    finalizedCheckpointPath);
        }

        // Format:
        // <global statistics>
        // <superstep classes>
        // <number of files>
        // <used file prefix 0><used file prefix 1>...
        // <aggregator data>
        // <masterCompute data>
        FSDataOutputStream finalizedOutputStream =
                getFs().create(finalizedCheckpointPath);

        String superstepFinishedNode = getBspService().
                getSuperstepFinishedPath(getBspService().getApplicationAttempt(), superstep - 1);

        finalizedOutputStream.write(
                getBspService().getZkExt().getData(superstepFinishedNode, false, null));

        finalizedOutputStream.writeInt(chosenWorkerInfoList.size());

        finalizedOutputStream.writeUTF("ignore"); //backwards compatibility

        for (WorkerInfo chosenWorkerInfo : chosenWorkerInfoList) {
            finalizedOutputStream.writeInt(chosenWorkerInfo.getWorkerIndex());
        }

        getCentralizedServiceMaster().getGlobalCommHandler().getAggregatorHandler().write(finalizedOutputStream);
        getCentralizedServiceMaster().getAggregatorTranslationHandler().write(finalizedOutputStream);
        getCentralizedServiceMaster().getMasterCompute().write(finalizedOutputStream);
        finalizedOutputStream.close();

        GiraphStats.getInstance().
                getLastCheckpointedSuperstep().setValue(superstep);

        //S3Checkpointer.upload(superstep, getConfig());
    }
}
