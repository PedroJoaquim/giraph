package org.apache.giraph.examples.pr;


import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
@Algorithm(
        name = "Page rank"
)
public class SimplePRComputation extends BasicComputation<LongWritable,
        DoubleWritable, FloatWritable, DoubleWritable> {
    /** Number of supersteps for this test */
    public static final int MAX_SUPERSTEPS = 30;

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
        if (getSuperstep() >= 1) {
            double sum = 0;
            for (DoubleWritable message : messages) {
                sum += message.get();
            }
            DoubleWritable vertexValue =
                    new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
            vertex.setValue(vertexValue);
        }

        if (getSuperstep() < MAX_SUPERSTEPS) {
            long edges = vertex.getNumEdges();
            sendMessageToAllEdges(vertex,
                    new DoubleWritable(vertex.getValue().get() / edges));
        } else {
            vertex.voteToHalt();
        }
    }

    /**
     * Simple VertexOutputFormat that supports {@link SimplePRComputation}
     */
    public static class SimplePageRankVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {
        @Override
        public TextVertexWriter createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new SimplePageRankVertexWriter();
        }

        /**
         * Simple VertexWriter that supports {@link SimplePRComputation}
         */
        public class SimplePageRankVertexWriter extends TextVertexWriter {
            @Override
            public void writeVertex(
                    Vertex<LongWritable, DoubleWritable, FloatWritable> vertex)
                    throws IOException, InterruptedException {
                getRecordWriter().write(
                        new Text(vertex.getId().toString()),
                        new Text(vertex.getValue().toString()));
            }
        }
    }
}
