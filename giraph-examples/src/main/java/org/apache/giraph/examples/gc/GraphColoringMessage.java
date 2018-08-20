package org.apache.giraph.examples.gc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GraphColoringMessage implements Writable {

    private long vertexId;

    public GraphColoringMessage() {
    }

    public GraphColoringMessage(long vertexId) {
        this.vertexId = vertexId;
    }

    public long getVertexId() {
        return vertexId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(vertexId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.vertexId = dataInput.readLong();
    }
}
