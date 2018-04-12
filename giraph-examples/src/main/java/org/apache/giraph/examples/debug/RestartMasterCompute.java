package org.apache.giraph.examples.debug;

import org.apache.giraph.master.MasterCompute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RestartMasterCompute extends MasterCompute{

    @Override
    public void compute() {
        if(getSuperstep()==2){
            haltComputation();
        }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
