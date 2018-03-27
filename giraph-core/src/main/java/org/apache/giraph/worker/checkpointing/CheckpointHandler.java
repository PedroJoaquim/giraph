package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class CheckpointHandler<I extends WritableComparable,
        V extends WritableComparable,
        E extends WritableComparable> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(CheckpointHandler.class);

    private CentralizedServiceWorker<I, V, E> centralizedServiceWorker;

    private BspService<I, V, E> bspService;

    public void initialize(CentralizedServiceWorker<I, V, E> centralizedServiceWorker, BspService<I, V, E> bspService){
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
     * Returns path to saved checkpoint.
     * Doesn't check if file actually exists.
     * @param superstep saved superstep.
     * @param name extension name
     * @return fill file path to checkpoint file
     */
    protected Path getSavedCheckpoint(long superstep, String name) {
        return new Path(this.bspService.getSavedCheckpointBasePath(superstep) + '.' + name);
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

    /**
     * Create checkpoint file safely. If file already exists remove it first.
     * @param name file extension
     * @return full file path to newly created file
     * @throws IOException
     */
    protected Path createCheckpointFilePathSafe(String name) throws IOException {
        Path validFilePath = new Path(getBspService().
                getCheckpointBasePath(getBspService().getSuperstep()) + '.' + name);
        // Remove these files if they already exist (shouldn't though, unless
        // of previous failure of this worker)

        getBspService().getFs().delete(validFilePath, false);

        return validFilePath;
    }



    protected ImmutableClassesGiraphConfiguration<I, V, E> getConfig(){
        return getCentralizedServiceWorker().getConfiguration();
    }

    protected Mapper<?, ?, ?, ?>.Context getContext(){
        return getBspService().getContext();
    }
}
