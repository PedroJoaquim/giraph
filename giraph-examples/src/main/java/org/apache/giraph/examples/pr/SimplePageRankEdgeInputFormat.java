package org.apache.giraph.examples.pr;


import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

public class SimplePageRankEdgeInputFormat extends
        TextEdgeInputFormat<LongWritable, FloatWritable> {
    /**
     * Splitter for endpoints
     */
    private final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public EdgeReader<LongWritable, FloatWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SimplePageRankEdgeReader();
    }

    public class SimplePageRankEdgeReader extends
            TextEdgeReaderFromEachLineProcessed<String[]> {

        private FloatWritable edgeValue = new FloatWritable(1f);

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
        protected FloatWritable getValue(String[] line) throws IOException {
            return edgeValue;
        }
    }

}
