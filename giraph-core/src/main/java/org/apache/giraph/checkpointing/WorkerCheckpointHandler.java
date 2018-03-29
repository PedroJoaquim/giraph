package org.apache.giraph.checkpointing;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class WorkerCheckpointHandler
        <I extends WritableComparable,
        V extends Writable,
        E extends Writable> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(WorkerCheckpointHandler.class);

    private CentralizedServiceWorker<I, V, E> centralizedServiceWorker;

    private BspService<I, V, E> bspService;

    private CheckpointPathManager pathManager;

    public void initialize(CentralizedServiceWorker<I, V, E> centralizedServiceWorker,
                           BspService<I, V, E> bspService,
                           CheckpointPathManager checkpointPathManager){

        this.pathManager = checkpointPathManager;
        this.centralizedServiceWorker = centralizedServiceWorker;
        this.bspService = bspService;
    }

    public abstract VertexEdgeCount loadCheckpoint(long superstep);

    public abstract void storeCheckpoint() throws IOException;


    protected CentralizedServiceWorker<I, V, E> getCentralizedServiceWorker() {
        return centralizedServiceWorker;
    }

    public BspService<I, V, E> getBspService() {
        return bspService;
    }

    /**
     * write the time to  restart from checkpoint to hdfs
     * @param timeToReadCheckpoint
     */
    protected void writeRestartFromCheckpointTime(double timeToReadCheckpoint) {

        int workerID = this.centralizedServiceWorker.getWorkerInfo().getTaskId();

        Path path = CheckpointingUtils.getCheckpointRestartInfoFilePath(workerID);

        try {
            FSDataOutputStream fileStream = this.bspService.getFs().create(path);
            fileStream.writeDouble(timeToReadCheckpoint);
            fileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    protected ImmutableClassesGiraphConfiguration<I, V, E> getConfig(){
        return getCentralizedServiceWorker().getConfiguration();
    }

    protected Mapper<?, ?, ?, ?>.Context getContext(){
        return getBspService().getContext();
    }

    protected Path deleteAndCreateCheckpointFilePath(String fullPath) throws IOException {
        Path validFilePath = new Path(fullPath);
        // Remove these files if they already exist (shouldn't though, unless
        // of previous failure of this worker)

        getBspService().getFs().delete(validFilePath, false);

        return validFilePath;
    }

    public CheckpointPathManager getPathManager() {
        return pathManager;
    }
}
