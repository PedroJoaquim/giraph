package org.apache.giraph.metis;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.BufferedWriter;
import java.io.IOException;

public class METISVertexCheckpointWriter extends
        VertexCheckpointWriter<LongWritable, Writable, Writable, Writable> {


    @Override
    public void writeVertex(Vertex<LongWritable, Writable, Writable> vertex,
                            BufferedWriter verticesStream) throws IOException {

        verticesStream.write(vertex.getId().toString());

        for (Edge<LongWritable, Writable> edge : vertex.getEdges()) {
            verticesStream.write(SEPARATOR);
            verticesStream.write(edge.getTargetVertexId().toString());

        }

        verticesStream.write(EOL);
    }

    @Override
    protected String writeVertexMessage(Writable vertexMessage) {
        return "";
    }

    @Override
    protected String writeVertexEdge(Edge<LongWritable, Writable> edge) {
        return "";
    }

    @Override
    protected String writeVertexValue(Writable value) {
        return "";
    }

    @Override
    protected String writeVertexID(LongWritable id) {
        return "";
    }

    @Override
    protected LongWritable readVertexID(String id) {
        return null;
    }

    @Override
    protected Writable readVertexValue(String value) {
        return null;
    }

    @Override
    protected Edge<LongWritable, Writable> readEdge(String edge) {
        return null;
    }

    @Override
    protected Writable readMessage(String msg) {
        return null;
    }
}
