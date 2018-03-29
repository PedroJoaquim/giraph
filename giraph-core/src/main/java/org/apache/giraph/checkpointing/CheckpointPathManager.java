package org.apache.giraph.checkpointing;

import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

public abstract class CheckpointPathManager {

    private final String savedCheckpointBasePath;

    private final String checkpointBasePath;

    public CheckpointPathManager(String savedCheckpointBasePath, String checkpointBasePath) {
        this.savedCheckpointBasePath = savedCheckpointBasePath;
        this.checkpointBasePath = checkpointBasePath;
    }

    public abstract String getVerticesCheckpointFilesDirPath(long superstep, boolean saved);

    public abstract String getMessagesCheckpointFilesDirPath(long superstep, boolean saved);

    public abstract String getWorkerMetadataCheckpointFileDirPath(long superstep, boolean saved);

    public abstract String getFinalizedCheckpointFileDirPath(long superstep, boolean saved);

    public abstract String getValidCheckpointFileDirPath(long superstep, boolean saved);

    public abstract String createVerticesCheckpointFilePath(long superstep, boolean saved, long partitionId);

    public abstract String createMessagesCheckpointFilePath(long superstep, boolean saved, long partitionId);

    public abstract String createValidCheckpointFilePath(long superstep, boolean saved, int workerId);

    public abstract String createMetadataCheckpointFilePath(long superstep, boolean saved, int workerId);

    public abstract String createFinalizedCheckpointFilePath(long superstep, boolean saved);

    public String getSavedCheckpointBasePath() {
        return savedCheckpointBasePath;
    }

    public String getCheckpointBasePath() {
        return checkpointBasePath;
    }

    public abstract long getLastCheckpointedSuperstep(FileSystem fs) throws IOException;

    /**
     * Only get the finalized checkpoint files
     */
    public static class FinalizedCheckpointPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            return path.getName().endsWith(CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX);
        }

    }
}
