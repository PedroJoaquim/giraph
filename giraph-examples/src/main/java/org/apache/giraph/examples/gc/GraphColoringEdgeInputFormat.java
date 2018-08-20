package org.apache.giraph.examples.gc;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

public class GraphColoringEdgeInputFormat extends
        TextEdgeInputFormat<LongWritable, NullWritable> {

    private final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new GraphColoringEdgeInputReader();
    }

    private class GraphColoringEdgeInputReader extends
            TextEdgeReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return SEPARATOR.split(line.toString());
        }

        @Override
        protected LongWritable getTargetVertexId(String[] line) throws IOException {
            return new LongWritable(Long.valueOf(line[1]));
        }

        @Override
        protected LongWritable getSourceVertexId(String[] line) throws IOException {
            return new LongWritable(Long.valueOf(line[0]));
        }

        @Override
        protected NullWritable getValue(String[] line) throws IOException {
            return NullWritable.get();
        }
    }
}
