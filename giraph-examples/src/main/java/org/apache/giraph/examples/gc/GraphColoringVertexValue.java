package org.apache.giraph.examples.gc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GraphColoringVertexValue implements Writable {

    private int degree;

    private int color;

    private int type;

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public int getColor() {
        return color;
    }

    public void setColor(int color) {
        this.color = color;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(degree);
        dataOutput.writeInt(color);
        dataOutput.writeInt(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.degree = dataInput.readInt();
        this.color = dataInput.readInt();
        this.type = dataInput.readInt();
    }
}
