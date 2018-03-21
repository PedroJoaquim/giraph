package org.apache.giraph.examples.mssp;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.TextAppendAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MSSPMasterCompute extends MasterCompute {

    private int maxEpoch;

    @Override
    public void compute() {

        long aggregatedValue = this.<LongWritable>getAggregatedValue(MultipleSourcesShortestPaths.MESSAGES_SENT_AGG).get();

        if(aggregatedValue == 0){
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

        long inc = MultipleSourcesShortestPaths.INC_LANDMARKS.get(getConf());

        aggregatorValue.append(1);

        for (int i = 1; i < numLandMarks; i++) {
            aggregatorValue.append(MultipleSourcesShortestPaths.AGGREGATOR_SEPARATOR).append(i * inc);
        }

        setAggregatedValue(MultipleSourcesShortestPaths.LANDMARK_VERTICES_AGG, new Text(aggregatorValue.toString()));

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
