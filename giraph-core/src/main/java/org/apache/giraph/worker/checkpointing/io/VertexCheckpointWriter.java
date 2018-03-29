package org.apache.giraph.worker.checkpointing.io;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public abstract class VertexCheckpointWriter<I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable> {

    private static final String SEPARATOR = "\t";

    private static final String EOL = "\n";

    private final Pattern SEPARATOR_PATTERN = Pattern.compile("[\t ]");

    private ImmutableClassesGiraphConfiguration<I, V, E> config;


    public void writeVertex(Vertex<I, V, E> vertex,
                            BufferedWriter verticesStream) throws IOException {

        verticesStream.write(writeVertexID(vertex.getId()));
        verticesStream.write(SEPARATOR);
        verticesStream.write(writeVertexValue(vertex.getValue()));
        verticesStream.write(SEPARATOR);
        verticesStream.write(Integer.toString(vertex.getNumEdges()));

        for (Edge<I, E> edge : vertex.getEdges()) {
            verticesStream.write(SEPARATOR);
            verticesStream.write(writeVertexEdge(edge));

        }

        verticesStream.write(EOL);
    }

    public void writeMessagesForVertex(I targetVertexId,
                              Iterable<M> messages,
                              BufferedWriter msgsStream) throws IOException {

        msgsStream.write(writeVertexID(targetVertexId));

        for (M msg : messages) {
            msgsStream.write(SEPARATOR);
            msgsStream.write(writeVertexMessage(msg));
        }

        msgsStream.write(EOL);
    }

    public Vertex<I, V, E>  readVertex(String line) {

        Vertex<I, V, E> vertex =  this.config.createVertex();

        vertex.setConf(config);

        String[] splits = SEPARATOR_PATTERN.split(line);

        I vertexID = readVertexID(splits[0]);

        V vertexValue = readVertexValue(splits[1]);

        int numEdges = Integer.parseInt(splits[2]);

        List<Edge<I, E>> edgeList = new ArrayList<>();

        for (int i = 3; i < numEdges + 3; i++) {
            edgeList.add(readEdge(splits[i]));
        }

        vertex.initialize(vertexID, vertexValue, edgeList);

        return vertex;
    }

    public MessagesInfo<I, M> readMessagesAndTarget(String line){

        String[] splits = SEPARATOR_PATTERN.split(line);

        I vertexID = readVertexID(splits[0]);

        List<M> messages = new ArrayList<>();

        for (int i = 1; i < splits.length; i++) {
            messages.add(readMessage(splits[i]));
        }

        return new MessagesInfo<I, M>(vertexID, messages);
    }


    protected abstract String writeVertexMessage(M vertexMessage);

    protected abstract String writeVertexEdge(Edge<I, E> edge);

    protected abstract String writeVertexValue(V value);

    protected abstract String writeVertexID(I id);

    protected abstract I readVertexID(String id);

    protected abstract V readVertexValue(String value);

    protected abstract Edge<I, E> readEdge(String edge);

    protected abstract M readMessage(String msg);


    public void initialize(ImmutableClassesGiraphConfiguration<I, V, E> config) {
        this.config = config;
    }

    public class MessagesInfo
            <I2 extends WritableComparable,
            M2 extends Writable>{


        private I2 vertexId;

        private List<M2> messages;

        public MessagesInfo(I2 vertexId, List<M2> messages) {
            this.vertexId = vertexId;
            this.messages = messages;
        }

        public I2 getVertexId() {
            return vertexId;
        }

        public List<M2> getMessages() {
            return messages;
        }
    }
}
