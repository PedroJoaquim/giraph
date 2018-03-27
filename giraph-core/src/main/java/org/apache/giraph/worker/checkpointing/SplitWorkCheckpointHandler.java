package org.apache.giraph.worker.checkpointing;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;


public abstract class SplitWorkCheckpointHandler
        <I extends WritableComparable,
        V extends WritableComparable,
        E extends WritableComparable> extends DefaultCheckpointHandler<I, V, E>{

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(SplitWorkCheckpointHandler.class);

    @Override
    protected void storeVerticesAndMessages(){

        final int numPartitions = getCentralizedServiceWorker().getPartitionStore().getNumPartitions();

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(getConfig()),
                numPartitions);

        getCentralizedServiceWorker().getPartitionStore().startIteration();

        final CompressionCodec codec =
                new CompressionCodecFactory(getConfig())
                        .getCodec(new Path(
                                GiraphConstants.CHECKPOINT_COMPRESSION_CODEC
                                        .get(getConfig())));


        long t0 = System.currentTimeMillis();

        CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
            @Override
            public Callable<Void> newCallable(int callableId) {
                return new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {
                        while (true) {
                            Partition<I, V, E> partition =
                                    getCentralizedServiceWorker().getPartitionStore().getNextPartition();

                            if (partition == null) {
                                break;
                            }
                            Path verticesPath =
                                    createCheckpointFilePathSafe( partition.getId() +
                                            CheckpointingUtils.CHECKPOINT_VERTICES_POSTFIX);

                            Path msgsPath =
                                    createCheckpointFilePathSafe( partition.getId() +
                                            CheckpointingUtils.CHECKPOINT_MESSAGES_POSTFIX);

                            FSDataOutputStream uncompressedVerticesStream =
                                    getBspService().getFs().create(verticesPath);

                            FSDataOutputStream uncompressedMsgsStream =
                                    getBspService().getFs().create(msgsPath);

                            //TODO possibly remove coded because of lines ?
                            DataOutputStream verticesStream = codec == null ? uncompressedVerticesStream :
                                    new DataOutputStream(
                                            codec.createOutputStream(uncompressedVerticesStream));

                            DataOutputStream msgsStream = codec == null ? uncompressedMsgsStream :
                                    new DataOutputStream(
                                            codec.createOutputStream(uncompressedMsgsStream));


                            writePartitionToStream(verticesStream, partition);

                            getCentralizedServiceWorker().getPartitionStore().putPartition(partition);

                            writeMessages(msgsStream, getCentralizedServiceWorker().getServerData()
                                    .getCurrentMessageStore(), partition.getId());

                            getContext().progress();

                            verticesStream.close();
                            msgsStream.close();

                            uncompressedVerticesStream.close();
                            uncompressedMsgsStream.close();
                        }
                        return null;
                    }
                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "checkpoint-vertices-%d", getContext());

        LOG.info("Save checkpoint in " + (System.currentTimeMillis() - t0) +
                " ms, using " + numThreads + " threads");

    }

    protected abstract void writePartitionToStream(DataOutputStream verticesStream, Partition<I, V, E> partition) throws IOException;

    protected abstract void writeMessages(DataOutputStream msgsStream, MessageStore<I, Writable> currentMessageStore, int id) throws IOException;

}
