package org.apache.giraph.worker.checkpointing.io;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;

public abstract class VertexCheckpointHandler<I extends WritableComparable,
        V extends Writable,
        E extends Writable> {

    private static final String SEPARATOR = "\t";

    private static final String EOL = "\n";

    private final Pattern SEPARATOR_PATTERN = Pattern.compile("[\t ]");

    public void writeVertex(Vertex<I, V, E> vertex, DataOutputStream verticesStream) throws IOException {

        verticesStream.writeUTF(writeVertexID(vertex.getId()));
        verticesStream.writeUTF(SEPARATOR);
        verticesStream.writeUTF(writeVertexValue(vertex.getValue()));

        for (Edge<I, E> edge : vertex.getEdges()) {
            verticesStream.writeUTF(SEPARATOR);
            verticesStream.writeUTF(writeVertexEdge(edge));
        }

        verticesStream.writeUTF(EOL);
    }

    public Vertex<I, V, E> readVertex(String line) {
        return null;
    }

    protected abstract String writeVertexEdge(Edge<I, E> edge);

    protected abstract String writeVertexValue(V value);

    protected abstract String writeVertexID(I id);


}
