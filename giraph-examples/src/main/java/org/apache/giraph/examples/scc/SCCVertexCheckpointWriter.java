package org.apache.giraph.examples.scc;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.worker.checkpointing.io.VertexCheckpointWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class SCCVertexCheckpointWriter
        extends VertexCheckpointWriter<LongWritable, SccVertexValue, NullWritable, LongWritable> {


    private static final String VALUE_SEPARATOR = "#";

    @Override
    protected String writeVertexMessage(LongWritable vertexMessage) {
        return vertexMessage.toString();
    }

    @Override
    protected String writeVertexEdge(Edge<LongWritable, NullWritable> edge) {
        return edge.getTargetVertexId().toString();
    }

    @Override
    protected String writeVertexValue(SccVertexValue value) {

        StringBuilder sb = new StringBuilder();

        LongArrayList parents = value.getParents();

        sb.append(value.get());
        sb.append(VALUE_SEPARATOR);

        int size = parents == null ? 0 : parents.size();

        sb.append(size);

        if(size > 0){
            for(long incomingId : parents){
                sb.append(VALUE_SEPARATOR);
                sb.append(incomingId);
            }
        }

        sb.append(VALUE_SEPARATOR);
        sb.append(value.isActive());

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
    protected SccVertexValue readVertexValue(String value) {

        SccVertexValue vertexValue = new SccVertexValue();

        String[] valueSplits = value.split(VALUE_SEPARATOR);

        vertexValue.set(Long.parseLong(valueSplits[0]));

        int size = Integer.parseInt(valueSplits[1]);

        if (size > 0) {
            for (int i = 0; i < size; i++) {
                vertexValue.addParent(Long.parseLong(valueSplits[2 + i]));
            }
        }

        if(!Boolean.parseBoolean(valueSplits[valueSplits.length - 1])){
            vertexValue.deactivate();
        }

        return vertexValue;
    }

    @Override
    protected Edge<LongWritable, NullWritable> readEdge(String edge) {
        return EdgeFactory.create(new LongWritable(Long.parseLong(edge)), NullWritable.get());
    }

    @Override
    protected LongWritable readMessage(String msg) {
        return new LongWritable(Long.parseLong(msg));
    }
}
