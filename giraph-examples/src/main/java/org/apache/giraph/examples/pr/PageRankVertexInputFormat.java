package org.apache.giraph.examples.pr;


import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class PageRankVertexInputFormat extends
        TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {
    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new PageRankTextVertexReader();
    }

    public class PageRankTextVertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {

        private final Pattern SEPARATOR = Pattern.compile("[\t ]");

        private DoubleWritable vertexValue = new DoubleWritable(10d);

        private FloatWritable edgeValue = new FloatWritable(1f);

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return SEPARATOR.split(line.toString());
        }

        @Override
        protected LongWritable getId(String[] line) throws IOException {
            return new LongWritable(Long.parseLong(line[0]));
        }

        @Override
        protected DoubleWritable getValue(String[] line) throws IOException {
            return vertexValue;
        }

        @Override
        protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(String[] line) throws IOException {
            List<Edge<LongWritable, FloatWritable>> edges =
                    Lists.newArrayListWithCapacity(line.length - 1);
            for (int n = 1; n < line.length; n++) {
                edges.add(EdgeFactory.create(
                        new LongWritable(Long.parseLong(line[n])), edgeValue));
            }
            return edges;
        }
    }
}
