package org.apache.giraph.examples.mssp;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MSSPVertexOutputFormat
        extends TextVertexOutputFormat<LongWritable, ArrayWritable<DoubleWritable>, NullWritable> {

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
        return new MSSPValueVertexWriter();
    }

    protected class MSSPValueVertexWriter extends TextVertexWriterToEachLine {

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
        protected Text convertVertexToLine(Vertex<LongWritable, ArrayWritable<DoubleWritable>, NullWritable> vertex) throws IOException {

            StringBuilder str = new StringBuilder();
            if (reverseOutput) {
                str.append(convertVertexValueToString(vertex.getValue()));
                str.append(delimiter);
                str.append(vertex.getId().toString());
            } else {
                str.append(vertex.getId().toString());
                str.append(delimiter);
                str.append(convertVertexValueToString(vertex.getValue()));
            }

            return new Text(str.toString());
        }

        private String convertVertexValueToString(ArrayWritable<DoubleWritable> value){

            DoubleWritable[] valuesArray = value.get();

            StringBuilder str = new StringBuilder();
            str.append(Double.toString(valuesArray[0].get()));

            for (int i = 1; i < valuesArray.length; i++) {
                str.append(",").append(Double.toString(valuesArray[i].get()));
            }

            return str.toString();
        }
    }
}
