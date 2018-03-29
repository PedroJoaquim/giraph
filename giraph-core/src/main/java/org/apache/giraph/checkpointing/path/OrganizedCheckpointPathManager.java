package org.apache.giraph.checkpointing.path;

import org.apache.giraph.checkpointing.CheckpointPathManager;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

public class OrganizedCheckpointPathManager extends CheckpointPathManager {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(DefaultCheckpointPathManager.class);

    private static final String VERTICES_FORLD_NAME = "vertices";

    private static final String MESSAGES_FORLD_NAME = "messages";

    public OrganizedCheckpointPathManager(String savedCheckpointBasePath, String checkpointBasePath) {
        super(savedCheckpointBasePath, checkpointBasePath);
    }

    @Override
    public String getVerticesCheckpointFilesDirPath(long superstep, boolean saved) {

        String basePath = saved ?
                getSavedCheckpointBasePath() : getCheckpointBasePath();


        return basePath + "/" + superstep + "/" + VERTICES_FORLD_NAME + "/";
    }

    @Override
    public String getMessagesCheckpointFilesDirPath(long superstep, boolean saved) {

        String basePath = saved ?
                getSavedCheckpointBasePath() : getCheckpointBasePath();


        return basePath + "/" + superstep + "/" + MESSAGES_FORLD_NAME + "/";
    }

    @Override
    public String getWorkerMetadataCheckpointFileDirPath(long superstep, boolean saved) {

        String basePath = saved ?
                getSavedCheckpointBasePath() : getCheckpointBasePath();

        return basePath + "/" + superstep + "/";
    }

    @Override
    public String getFinalizedCheckpointFileDirPath(long superstep, boolean saved) {
        return getWorkerMetadataCheckpointFileDirPath(superstep, saved);
    }

    @Override
    public String getValidCheckpointFileDirPath(long superstep, boolean saved) {
        return getWorkerMetadataCheckpointFileDirPath(superstep, saved);
    }

    @Override
    public String createVerticesCheckpointFilePath(long superstep, boolean saved, long partitionId) {

        String basePath = getVerticesCheckpointFilesDirPath(superstep, saved);

        return basePath  + partitionId + CheckpointingUtils.CHECKPOINT_VERTICES_POSTFIX;
    }

    @Override
    public String createMessagesCheckpointFilePath(long superstep, boolean saved, long partitionId) {

        String basePath = getMessagesCheckpointFilesDirPath(superstep, saved);

        return basePath  + partitionId + CheckpointingUtils.CHECKPOINT_MESSAGES_POSTFIX;
    }

    @Override
    public String createValidCheckpointFilePath(long superstep, boolean saved, int workerId) {
        String basePath = getValidCheckpointFileDirPath(superstep, saved);

        return basePath + workerId + CheckpointingUtils.CHECKPOINT_VALID_POSTFIX;
    }

    @Override
    public String createMetadataCheckpointFilePath(long superstep, boolean saved, int workerId) {
        String basePath = getWorkerMetadataCheckpointFileDirPath(superstep, saved);

        return basePath + workerId + CheckpointingUtils.CHECKPOINT_METADATA_POSTFIX;
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

            FileStatus[] fileStatusArray = fs.listStatus(cpPath);

            if (fileStatusArray != null) {

                long lastCheckpointedSuperstep = Long.MIN_VALUE;

                for (FileStatus fileStatus : fileStatusArray) {
                    if(fileStatus.isDir() &&
                            containsFinalizedFile(fs, fileStatus)){

                        long superstep = readSuperstepFromDir(fileStatus);

                        if(superstep > lastCheckpointedSuperstep){
                            lastCheckpointedSuperstep = superstep;
                        }
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

    private long readSuperstepFromDir(FileStatus fileStatus) {
        return Long.valueOf(fileStatus.getPath().getName());
    }

    private boolean containsFinalizedFile(FileSystem fs, FileStatus fileStatus) throws IOException {

        FileStatus[] fileStatusArray =
                fs.listStatus(fileStatus.getPath(), new FinalizedCheckpointPathFilter());

        return fileStatusArray != null && fileStatusArray.length > 0;
    }
}
