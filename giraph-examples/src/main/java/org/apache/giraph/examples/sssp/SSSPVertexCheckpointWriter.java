package org.apache.giraph.examples.sssp;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;


public class SSSPVertexCheckpointWriter extends
        VertexCheckpointWriter<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(SSSPVertexCheckpointWriter.class);

    @Override
    protected String writeVertexMessage(DoubleWritable vertexMessage) {
        return vertexMessage.toString();
    }

    @Override
    protected String writeVertexEdge(Edge<LongWritable, NullWritable> edge) {
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
    protected Edge<LongWritable, NullWritable> readEdge(String edge) {
        return EdgeFactory.create(new LongWritable(Long.parseLong(edge)), NullWritable.get());
    }

    @Override
    protected DoubleWritable readMessage(String value) {
        return new DoubleWritable(Double.parseDouble(value));
    }
}
