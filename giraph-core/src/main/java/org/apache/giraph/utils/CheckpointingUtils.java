/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.utils;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;

import static org.apache.giraph.conf.GiraphConstants.CHECKPOINT_DIRECTORY;

/**
 * Holds useful functions to get checkpoint paths
 * in hdfs.
 */
public class CheckpointingUtils {

  /** If at the end of a checkpoint file, indicates metadata */
  public static final String CHECKPOINT_METADATA_POSTFIX = ".metadata";
  /**
   * If at the end of a checkpoint file, indicates vertices, edges,
   * messages, etc.
   */
  public static final String CHECKPOINT_VERTICES_POSTFIX = ".vertices";
  /**
   * If at the end of a checkpoint file, indicates metadata and data is valid
   * for the same filenames without .valid
   */
  public static final String CHECKPOINT_VALID_POSTFIX = ".valid";
  /**
   * If at the end of a checkpoint file,
   * indicates that we store WorkerContext and aggregator handler data.
   */
  public static final String CHECKPOINT_DATA_POSTFIX = ".data";
  /**
   * If at the end of a checkpoint file,
   * indicates that we store messages for partitions.
   */
  public static final String CHECKPOINT_MESSAGES_POSTFIX = ".messages";
  /**
   * If at the end of a checkpoint file, indicates the stitched checkpoint
   * file prefixes.  A checkpoint is not valid if this file does not exist.
   */
  public static final String CHECKPOINT_FINALIZED_POSTFIX = ".finalized";

  public static final String RESTART_CHECKPOINT_TIME_POSTFIX = ".restart";

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(CheckpointingUtils.class);


  /**
   * Do not call constructor.
   */
  private CheckpointingUtils() {
  }

  /**
   * Path to the checkpoint's root (including job id)
   * @param conf Immutable configuration of the job
   * @param jobId job ID
   * @return checkpoint's root
   */
  public static String getCheckpointBasePath(Configuration conf,
                                             String jobId) {
    return CHECKPOINT_DIRECTORY.getWithDefault(conf,
        CHECKPOINT_DIRECTORY.getDefaultValue() + "/" + jobId);
  }

  /**
   * Path to checkpoint&amp;halt node in hdfs.
   * It is set to let client know that master has
   * successfully finished checkpointing and job can be restarted.
   * @param conf Immutable configuration of the job
   * @param jobId job ID
   * @return path to checkpoint&amp;halt node in hdfs.
   */
  public static Path getCheckpointMarkPath(Configuration conf,
                                           String jobId) {
    return new Path(getCheckpointBasePath(conf, jobId), "halt");
  }


  public static Path getCheckpointRestartInfoFilePath(int workerID){
    return new Path("checkpoint_" + workerID + RESTART_CHECKPOINT_TIME_POSTFIX);
  }

  public static boolean isRestartInfoFile(Path file) {
    return file.getName().endsWith(RESTART_CHECKPOINT_TIME_POSTFIX);
  }

  public static  String getPartitionInfoPath(ImmutableClassesGiraphConfiguration configuration) {
   return "_bsp/_partitionInfo" + configuration.getJobId() + "/";
  }
}
