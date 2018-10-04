package org.apache.giraph.examples.gc;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GraphColoringMasterCompute extends MasterCompute {


    @Override
    public void compute() {
        if(getSuperstep() == 1000){
            haltComputation();
        }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {

        registerAggregator(GraphColoringComputation.UNKNOWN_VERTICES_REMAINING,
                IntSumAggregator.class);

        registerAggregator(GraphColoringComputation.UNASSIGNED_VERTICES_REMAINING,
                IntSumAggregator.class);

        setAggregatedValue(GraphColoringComputation.UNKNOWN_VERTICES_REMAINING,
                new IntWritable(0));

        setAggregatedValue(GraphColoringComputation.UNASSIGNED_VERTICES_REMAINING,
                new IntWritable(0));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }


}
