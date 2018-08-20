package org.apache.giraph.examples.gc;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;


public class GraphColoringVertexCheckpointWriter extends
        VertexCheckpointWriter<LongWritable, GraphColoringVertexValue, NullWritable, GraphColoringMessage> {


    private static final String VALUE_SEPARATOR = "#";

    private static final Logger LOG =
            Logger.getLogger(GraphColoringVertexCheckpointWriter.class);

    @Override
    protected String writeVertexMessage(GraphColoringMessage vertexMessage) {
        return Long.toString(vertexMessage.getVertexId());
    }

    @Override
    protected String writeVertexEdge(Edge<LongWritable, NullWritable> edge) {
        return edge.getTargetVertexId().toString();
    }

    @Override
    protected String writeVertexValue(GraphColoringVertexValue value) {
        return value.getType() + VALUE_SEPARATOR + value.getDegree() + VALUE_SEPARATOR + value.getColor();
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
    protected GraphColoringVertexValue readVertexValue(String value) {

        String[] splits = value.split(VALUE_SEPARATOR);

        GraphColoringVertexValue vertexValue = new GraphColoringVertexValue();
        vertexValue.setType(Integer.valueOf(splits[0]));
        vertexValue.setDegree(Integer.valueOf(splits[1]));
        vertexValue.setColor(Integer.valueOf(splits[2]));

        return vertexValue;
    }

    @Override
    protected Edge<LongWritable, NullWritable> readEdge(String edge) {
        return EdgeFactory.create(new LongWritable(Long.parseLong(edge)), NullWritable.get());
    }

    @Override
    protected GraphColoringMessage readMessage(String msg) {
        return new GraphColoringMessage(Long.valueOf(msg));
    }
}
