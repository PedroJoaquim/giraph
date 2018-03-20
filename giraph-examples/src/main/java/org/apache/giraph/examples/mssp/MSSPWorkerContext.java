package org.apache.giraph.examples.mssp;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class MSSPWorkerContext extends WorkerContext {

    private long[] landmarks;

    private int currentEpoch;

    private static final Logger LOG =
            Logger.getLogger(MSSPWorkerContext.class);

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {

        Text aggregatedValue = this.<Text>getAggregatedValue(MultipleSourcesShortestPaths.LANDMARK_VERTICES_AGG);

        LOG.info("debug-mssp: aggregatedValue = " + aggregatedValue);

        String[] splited = aggregatedValue.toString().split(MultipleSourcesShortestPaths.AGGREGATOR_SEPARATOR);

        this.landmarks = new long[splited.length];

        for (int i = 0; i < splited.length; i++) {
            this.landmarks[i] = Long.parseLong(splited[i]);
        }
    }

    @Override
    public void postApplication() {

    }

    @Override
    public void preSuperstep() {
        this.currentEpoch =  this.<IntWritable>getAggregatedValue(MultipleSourcesShortestPaths.CURRENT_EPOCH_AGG).get();
    }

    @Override
    public void postSuperstep() {

    }
}
