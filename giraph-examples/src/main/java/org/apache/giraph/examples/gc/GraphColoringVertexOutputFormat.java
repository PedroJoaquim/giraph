package org.apache.giraph.examples.gc;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class GraphColoringVertexOutputFormat
        extends TextVertexOutputFormat<LongWritable, GraphColoringVertexValue, NullWritable> {

    /** Specify the output delimiter */
    private static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /** Default output delimiter */
    private static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
    /** Reverse id and value order? */
    private static final String REVERSE_ID_AND_VALUE = "reverse.id.and.value";
    /** Default is to not reverse id and value order. */
    private static final boolean REVERSE_ID_AND_VALUE_DEFAULT = false;


    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new GraphColoringVertexWriter();
    }

    private class GraphColoringVertexWriter extends TextVertexWriterToEachLine {

        /** Saved delimiter */
        private String delimiter;
        /** Cached reserve option */
        private boolean reverseOutput;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(
                    LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            reverseOutput = getConf().getBoolean(
                    REVERSE_ID_AND_VALUE, REVERSE_ID_AND_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<LongWritable, GraphColoringVertexValue, NullWritable> vertex) throws IOException {
            StringBuilder str = new StringBuilder();
            if (reverseOutput) {
                str.append(vertex.getValue().getColor());
                str.append(delimiter);
                str.append(vertex.getId().toString());
            } else {
                str.append(vertex.getId().toString());
                str.append(delimiter);
                str.append(vertex.getValue().getColor());
            }

            return new Text(str.toString());
        }
    }
}
