package org.apache.giraph.metis;

import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class METISPartitionBalancer<I extends WritableComparable,
        V extends Writable, E extends Writable> {

    public abstract void reassignPartitions(BspServiceWorker<I, V, E> serviceWorker);
}
