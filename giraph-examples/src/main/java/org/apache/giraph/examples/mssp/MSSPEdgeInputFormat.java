package org.apache.giraph.examples.mssp;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

public class MSSPEdgeInputFormat extends
        TextEdgeInputFormat<LongWritable, NullWritable> {
    /**
     * Splitter for endpoints
     */
    private final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new MSSPEdgeReader();
    }

    public class MSSPEdgeReader extends
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
