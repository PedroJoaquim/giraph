package org.apache.giraph.examples.gc;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class GraphColoringVertexInputFormat extends
        TextVertexInputFormat<LongWritable, GraphColoringVertexValue, NullWritable> {


    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new GraphColoringVertexReader();
    }

    private class GraphColoringVertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {

        private final Pattern SEPARATOR = Pattern.compile("[\t ]");


        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return SEPARATOR.split(line.toString());
        }

        @Override
        protected LongWritable getId(String[] line) throws IOException {
            return new LongWritable(Long.parseLong(line[0]));
        }

        @Override
        protected GraphColoringVertexValue getValue(String[] line) throws IOException {
            return new GraphColoringVertexValue();
        }

        @Override
        protected Iterable<Edge<LongWritable, NullWritable>> getEdges(String[] line) throws IOException {
            List<Edge<LongWritable, NullWritable>> edges =
                    Lists.newArrayListWithCapacity(line.length - 1);
            for (int n = 1; n < line.length; n++) {
                edges.add(EdgeFactory.create(
                        new LongWritable(Long.parseLong(line[n])), NullWritable.get()));
            }
            return edges;
        }
    }
}
