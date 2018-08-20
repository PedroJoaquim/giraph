package org.apache.giraph.examples.mssp;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class MSSPVertexCheckpointWriter extends
        VertexCheckpointWriter<LongWritable, MSSPVertexValue, NullWritable, DoubleWritable>{

    private static final String VALUE_SEPARATOR = "#";

    @Override
    protected String writeVertexMessage(DoubleWritable vertexMessage) {
        return vertexMessage.toString();
    }

    @Override
    protected String writeVertexEdge(Edge<LongWritable, NullWritable> edge) {
        return edge.getTargetVertexId().toString();
    }

    @Override
    protected String writeVertexValue(MSSPVertexValue value) {

        StringBuilder sb = new StringBuilder();

        double[] values = value.getValues();

        int size = values.length;

        sb.append(size);

        if(size > 0){
            for (double itValue : values) {
                sb.append(VALUE_SEPARATOR);
                sb.append(itValue);
            }

        }

        return sb.toString();
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
    protected MSSPVertexValue readVertexValue(String value) {

        String[] splits = value.split(VALUE_SEPARATOR);

        int size = Integer.parseInt(splits[0]);

        if(size > 0){
            double[] values = new double[size];

            for (int i = 0; i < size; i++) {
                values[i] = Double.parseDouble(splits[1 + i]);
            }

            return new MSSPVertexValue(values, size);
        }
        else {
            return new MSSPVertexValue();
        }

    }

    @Override
    protected Edge<LongWritable, NullWritable> readEdge(String edge) {
        return EdgeFactory.create(new LongWritable(Long.parseLong(edge)), NullWritable.get());
    }

    @Override
    protected DoubleWritable readMessage(String msg) {
        return new DoubleWritable(Double.parseDouble(msg));
    }
}
