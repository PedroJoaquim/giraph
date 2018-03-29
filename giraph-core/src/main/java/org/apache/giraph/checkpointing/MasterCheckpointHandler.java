package org.apache.giraph.checkpointing;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public abstract class MasterCheckpointHandler
                <I extends WritableComparable,
                V extends Writable,
                E extends Writable> {


    /** Class logger */
    private static final Logger LOG = Logger.getLogger(MasterCheckpointHandler.class);

    private CentralizedServiceMaster<I, V, E> centralizedServiceMaster;

    private BspService<I, V, E> bspService;

    private CheckpointPathManager pathManager;

    public void initialize(CentralizedServiceMaster<I, V, E> centralizedServiceMaster,
                           BspService<I, V, E> bspService,
                           CheckpointPathManager checkpointPathManager){

        this.pathManager = checkpointPathManager;
        this.centralizedServiceMaster = centralizedServiceMaster;
        this.bspService = bspService;
    }

    public abstract Collection<PartitionOwner> prepareCheckpointRestart(long superstep) throws IOException;

    public abstract void finalizeCheckpoint(long superstep, List<WorkerInfo> chosenWorkerInfoList)
            throws IOException, KeeperException, InterruptedException;



    public CentralizedServiceMaster<I, V, E> getCentralizedServiceMaster() {
        return centralizedServiceMaster;
    }

    public BspService<I, V, E> getBspService() {
        return bspService;
    }

    public CheckpointPathManager getPathManager() {
        return pathManager;
    }

    protected ImmutableClassesGiraphConfiguration<I, V, E> getConfig(){
        return getCentralizedServiceMaster().getConfiguration();
    }


    protected FileSystem getFs(){
        return this.bspService.getFs();
    }


    protected void updateCounters(GlobalStats globalStats) {
        GiraphStats gs = GiraphStats.getInstance();
        gs.getVertices().setValue(globalStats.getVertexCount());
        gs.getFinishedVertexes().setValue(globalStats.getFinishedVertexCount());
        gs.getEdges().setValue(globalStats.getEdgeCount());
        gs.getSentMessages().setValue(globalStats.getMessageCount());
        gs.getSentMessageBytes().setValue(globalStats.getMessageBytesCount());
        gs.getAggregateSentMessages().increment(globalStats.getMessageCount());
        gs.getAggregateSentMessageBytes()
                .increment(globalStats.getMessageBytesCount());
        gs.getAggregateOOCBytesLoaded()
                .increment(globalStats.getOocLoadBytesCount());
        gs.getAggregateOOCBytesStored()
                .increment(globalStats.getOocStoreBytesCount());
        // Updating the lowest percentage of graph in memory throughout the
        // execution across all the supersteps
        int percentage = (int) gs.getLowestGraphPercentageInMemory().getValue();
        gs.getLowestGraphPercentageInMemory().setValue(
                Math.min(percentage, globalStats.getLowestGraphPercentageInMemory()));
    }


}
