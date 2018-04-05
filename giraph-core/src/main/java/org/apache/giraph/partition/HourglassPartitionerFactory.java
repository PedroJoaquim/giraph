package org.apache.giraph.partition;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;


public class HourglassPartitionerFactory<V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(HourglassPartitionerFactory.class);

    private Long2IntMap vertexToPartitionMapping;

    private boolean greedyMetisPartitioning;

    public HourglassPartitionerFactory() {
        this.greedyMetisPartitioning = false;
    }

    public void greedyPartitioningDone(BspService<LongWritable, V, E> service){

        long start = System.currentTimeMillis();
        readVertexToPartitionMapping(service);
        long end = System.currentTimeMillis();

        this.greedyMetisPartitioning = true;

        LOG.info("debug-metis: time to read vertex mapping from hdfs = " + (end -start)/1000.0d + " secs");
    }

    private void readVertexToPartitionMapping(BspService<LongWritable, V, E> service) {

        final Queue<FileStatus> fsQueue =
                new ConcurrentLinkedQueue<>();

        FileStatus[] fileStatuses;

        final Long2IntMap vertexMapping = new Long2IntOpenHashMap();

        final FileSystem fs = service.getFs();

        try {
            fileStatuses = fs.listStatus(new Path(service.getConfiguration().getVertexAssignmentPath()));
            fsQueue.addAll(Arrays.asList(fileStatuses));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(service.getConfiguration()),
                fileStatuses.length);


        CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
            @Override
            public Callable<Void> newCallable(int callableId) {
                return new Callable<Void>() {
                    @Override
                    public Void call() throws IOException {

                        while (!fsQueue.isEmpty()) {

                            FileStatus targetFile = fsQueue.poll();

                            if (targetFile == null) {
                                break;
                            }

                            Path targetPartitionInfoPath = targetFile.getPath();

                            if (!targetPartitionInfoPath.getName().endsWith(".mapping")) {
                                continue;
                            }

                            FSDataInputStream fileStream =
                                    fs.open(targetPartitionInfoPath);

                            int partitionId = fileStream.readInt();

                            long numVertices = fileStream.readLong();

                            long[] ids = new long[(int)numVertices];

                            for (int i = 0; i < numVertices; i++) {
                                ids[i] = fileStream.readLong();
                            }

                            fileStream.close();

                            synchronized (vertexMapping){
                                for (int i = 0; i < ids.length; i++) {
                                    vertexMapping.put(ids[i], partitionId);
                                }
                            }
                        }

                        return null;
                    }
                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "metis-read-%d", service.getContext());

        this.vertexToPartitionMapping = vertexMapping;
    }

    @Override
    public int getPartition(LongWritable id, int partitionCount, int workerCount) {

        if(greedyMetisPartitioning){
            return this.vertexToPartitionMapping.get(id.get());
        }
        else {
            return Math.abs(id.hashCode() % partitionCount);
        }
    }

    //give workers contiguous partitions
    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {

        if(getConf().isUndirectedGraph() && getConf().isMETISPartitioning()){
            int avgNumberPartitionsPerWork = partitionCount / workerCount;

            return Math.min(partition / avgNumberPartitionsPerWork, workerCount - 1);
        }
        else {
            return partition % workerCount;
        }
    }
}
