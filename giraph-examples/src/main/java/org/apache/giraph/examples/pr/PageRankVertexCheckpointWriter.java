package org.apache.giraph.examples.pr;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class PageRankVertexCheckpointWriter extends VertexCheckpointWriter <LongWritable,
        DoubleWritable,
        FloatWritable,
        DoubleWritable>{

    private static final FloatWritable edgeValue = new FloatWritable(1);

    @Override
    protected String writeVertexMessage(DoubleWritable vertexMessage) {
        return vertexMessage.toString();
    }

    @Override
    protected String writeVertexEdge(Edge<LongWritable, FloatWritable> edge) {
        return edge.getTargetVertexId().toString();
    }

    @Override
    protected String writeVertexValue(DoubleWritable value) {
        return value.toString();
    }

    @Override
    protected String writeVertexID(LongWritable id) {
        return id.toString();
    }

    @Override
    protected LongWritable readVertexID(String id) {
        return new LongWritable(Long.parseLong(id));
    }

    @Override
    protected DoubleWritable readVertexValue(String value) {
        return new DoubleWritable(Double.parseDouble(value));
    }

    @Override
    protected Edge<LongWritable, FloatWritable> readEdge(String edge) {
        return EdgeFactory.create(new LongWritable(Long.parseLong(edge)), edgeValue);
    }

    @Override
    protected DoubleWritable readMessage(String msg) {
        return new DoubleWritable(Double.parseDouble(msg));
    }
}
