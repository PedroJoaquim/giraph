package org.apache.giraph.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class GreedyHourglassPartitionerFactory<V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {


    @Override
    public int getPartition(LongWritable id, int partitionCount, int workerCount) {
        return 0;
    }

    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return 0;
    }
}
