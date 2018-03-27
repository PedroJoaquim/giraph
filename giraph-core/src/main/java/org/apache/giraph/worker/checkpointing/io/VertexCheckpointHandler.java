package org.apache.giraph.worker.checkpointing.io;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface VertexCheckpointHandler<I extends WritableComparable, V, E extends Writable> {

    String writeVertexID(I vertexID);

    String writeVertexValue(V vertexValue);

    String writeVertexEdge(Edge<I, E> edge);

    I readVertexID(String vertexID);

    V readVertexValue(String vertexID);

    Edge<I, E> readEdge(String edge);

}
