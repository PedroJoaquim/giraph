package org.apache.giraph.checkpointing.path;

import org.apache.giraph.checkpointing.CheckpointPathManager;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;

public class DefaultCheckpointPathManager extends CheckpointPathManager{

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(DefaultCheckpointPathManager.class);


    public DefaultCheckpointPathManager(String savedCheckpointBasePath, String checkpointBasePath) {
        super(savedCheckpointBasePath, checkpointBasePath);
    }

    @Override
    public String getVerticesCheckpointFilesDirPath(long superstep, boolean saved) {

        String basePath = saved ?
                getSavedCheckpointBasePath() : getCheckpointBasePath();

        return basePath + "/";
    }

    @Override
    public String getMessagesCheckpointFilesDirPath(long superstep, boolean saved) {
        return getVerticesCheckpointFilesDirPath(superstep, saved);
    }

    @Override
    public String getWorkerMetadataCheckpointFileDirPath(long superstep, boolean saved) {
        return getVerticesCheckpointFilesDirPath(superstep, saved);
    }

    @Override
    public String getFinalizedCheckpointFileDirPath(long superstep, boolean saved) {
        return getVerticesCheckpointFilesDirPath(superstep, saved);
    }

    @Override
    public String getValidCheckpointFileDirPath(long superstep, boolean saved) {
        return getVerticesCheckpointFilesDirPath(superstep, saved);
    }

    @Override
    public String createVerticesCheckpointFilePath(long superstep, boolean saved, long partitionId) {

        String basePath = getVerticesCheckpointFilesDirPath(superstep, saved);

        return basePath + superstep + '.' + partitionId + CheckpointingUtils.CHECKPOINT_VERTICES_POSTFIX;
    }

    @Override
    public String createMessagesCheckpointFilePath(long superstep, boolean saved, long partitionId) {

        String basePath = getMessagesCheckpointFilesDirPath(superstep, saved);

        return basePath + superstep + '.' + partitionId + CheckpointingUtils.CHECKPOINT_MESSAGES_POSTFIX;
    }

    @Override
    public String createValidCheckpointFilePath(long superstep, boolean saved, int workerId) {

        String basePath = getValidCheckpointFileDirPath(superstep, saved);

        return basePath + superstep + '.' + workerId + CheckpointingUtils.CHECKPOINT_VALID_POSTFIX;
    }

    @Override
    public String createMetadataCheckpointFilePath(long superstep, boolean saved, int workerId) {

        String basePath = getWorkerMetadataCheckpointFileDirPath(superstep, saved);

        return basePath + superstep + '.' + workerId + CheckpointingUtils.CHECKPOINT_METADATA_POSTFIX;
    }

    @Override
    public String createFinalizedCheckpointFilePath(long superstep, boolean saved) {

        String basePath = getFinalizedCheckpointFileDirPath(superstep, saved);

        return basePath + superstep + CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX;
    }

    @Override
    public long getLastCheckpointedSuperstep(FileSystem fs) throws IOException {

        Path cpPath = new Path(getSavedCheckpointBasePath() + "/");

        if (fs.exists(cpPath)) {
            FileStatus[] fileStatusArray =
                    fs.listStatus(cpPath, new FinalizedCheckpointPathFilter());
            if (fileStatusArray != null) {
                long lastCheckpointedSuperstep = Long.MIN_VALUE;
                for (FileStatus file : fileStatusArray) {
                    long superstep = getSuperstepFromFinalizedFile(file);
                    if (superstep > lastCheckpointedSuperstep) {
                        lastCheckpointedSuperstep = superstep;
                    }
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("getLastGoodCheckpoint: Found last good checkpoint " +
                            lastCheckpointedSuperstep);
                }
                return lastCheckpointedSuperstep;
            }
        }
        return -1;
    }

    /**
     * Get the checkpoint from a finalized checkpoint path
     *
     * @param finalizedPath Path of the finalized checkpoint
     * @return Superstep referring to a checkpoint of the finalized path
     */
    protected static long getSuperstepFromFinalizedFile(FileStatus finalizedPath) {
        if (!finalizedPath.getPath().getName().
                endsWith(CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX)) {
            throw new InvalidParameterException(
                    "getCheckpoint: " + finalizedPath + "Doesn't end in " +
                            CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX);
        }
        String checkpointString =
                finalizedPath.getPath().getName().
                        replace(CheckpointingUtils.CHECKPOINT_FINALIZED_POSTFIX, "");

        return Long.parseLong(checkpointString);
    }

}
