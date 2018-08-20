package org.apache.giraph.examples.mssp;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.TextAppendAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class MSSPMasterCompute extends MasterCompute {

    private int maxEpoch;

    private static final Logger LOG =
            Logger.getLogger(MSSPMasterCompute.class);

    @Override
    public void compute() {

        long aggregatedValue = this.<LongWritable>getAggregatedValue(MultipleSourcesShortestPaths.MESSAGES_SENT_AGG).get();

        if(aggregatedValue == 0 && getSuperstep() > 0){

            int currentEpoch = this.<IntWritable>getAggregatedValue(MultipleSourcesShortestPaths.CURRENT_EPOCH_AGG).get();

            if(currentEpoch == maxEpoch){
                haltComputation();
            }
            else {
                setAggregatedValue(MultipleSourcesShortestPaths.CURRENT_EPOCH_AGG, new IntWritable(currentEpoch + 1));
            }
        }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {

        registerPersistentAggregator(MultipleSourcesShortestPaths.LANDMARK_VERTICES_AGG, TextAppendAggregator.class);
        registerPersistentAggregator(MultipleSourcesShortestPaths.CURRENT_EPOCH_AGG, IntSumAggregator.class);
        registerAggregator(MultipleSourcesShortestPaths.MESSAGES_SENT_AGG, LongSumAggregator.class);

        StringBuilder aggregatorValue = new StringBuilder();

        int numLandMarks = MultipleSourcesShortestPaths.NUM_LANDMARKS.get(getConf());

        this.maxEpoch = numLandMarks - 1;

        long maxLandmarkId = MultipleSourcesShortestPaths.MAX_LANDMARK_ID.get(getConf());

        generateRandomLandmarkIds(numLandMarks, maxLandmarkId, aggregatorValue);

        setAggregatedValue(MultipleSourcesShortestPaths.LANDMARK_VERTICES_AGG, new Text(aggregatorValue.toString()));

    }

    private void generateRandomLandmarkIds(int numLandMarks, long maxLandmarkId, StringBuilder aggregatorValue) {

        aggregatorValue.append(1);

        //for now just consider sequential ids
        for (int i = 2; i <= numLandMarks; i++) {
            aggregatorValue.append(MultipleSourcesShortestPaths.AGGREGATOR_SEPARATOR).append(i);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //TODO
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //TODO
    }
}
