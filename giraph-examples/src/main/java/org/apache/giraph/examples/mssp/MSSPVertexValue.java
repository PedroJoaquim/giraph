package org.apache.giraph.examples.mssp;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MSSPVertexValue implements Writable {

    /**distances to the source vertices*/
    private double[] values;

    /** num source vertices*/
    private int size;

    public MSSPVertexValue() {
    }

    public MSSPVertexValue(int arraySize) {
        this.values = new double[arraySize];
        this.size = arraySize;
    }

    public MSSPVertexValue(double[] values, int size) {
        this.values = values;
        this.size = size;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);

        for (double d : values) {
            out.writeDouble(d);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.size = in.readInt();

        this.values = new double[this.size];

        for (int i = 0; i < this.size; i++) {
            this.values[i] = in.readDouble();
        }
    }

    public void setValue(int index, double value){
        this.values[index] = value;
    }

    public double getValue(int index){
        return this.values[index];
    }

    public double[] getValues() {
        return values;
    }
}
