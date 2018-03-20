package org.apache.giraph.examples.mssp;

import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

@Algorithm(
        name = "Multiple Source Shortest paths",
        description = "Finds all shortest paths from a selected set of vertex"
)

public class MultipleSourcesShortestPaths
        extends BasicComputation<LongWritable, ArrayWritable, NullWritable, DoubleWritable>{

    public static final String LANDMARK_VERTICES_AGG = "landmarks";

    public static final String MESSAGES_SENT_AGG = "messages";

    public static final String CURRENT_EPOCH_AGG = "epoch";

    /** Number of landmarks to compute */
    public static final IntConfOption NUM_LANDMARKS =
            new IntConfOption("MultipleShortestPaths.numLandmarks", 30,
                    "number of landmarks");

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(MultipleSourcesShortestPaths.class);


    public static final String AGGREGATOR_SEPARATOR = "#";

    @Override
    public void compute(Vertex<LongWritable, ArrayWritable, NullWritable> vertex, Iterable<DoubleWritable> messages)
            throws IOException {

        MSSPWorkerContext workerContext = this.<MSSPWorkerContext>getWorkerContext();

        if(getSuperstep() == 0){
            DoubleWritable[] initialValues = new DoubleWritable[workerContext.getNumLandmarks()];
            Arrays.fill(initialValues, new DoubleWritable(Double.MAX_VALUE));

            vertex.setValue(new ArrayWritable(DoubleWritable.class, initialValues));
        }

        double minDist =
                workerContext.isSourceVertexForCurrentEpoch(vertex.getId().get()) ? 0d : Double.MAX_VALUE;

        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }

        if (minDist < getCurrentEpochValue(vertex, workerContext)) {
            setValue(new DoubleWritable(minDist), vertex, workerContext);
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                double distance = minDist + 1;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Vertex " + vertex.getId() + " sent to " +
                            edge.getTargetVertexId() + " = " + distance);
                }
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
            }
        }

        voteToHaltIfNecessary(vertex, workerContext);
    }

    private void voteToHaltIfNecessary(Vertex<LongWritable, ArrayWritable, NullWritable> vertex,
                                       MSSPWorkerContext workerContext) {

        /*
         * We only vote to halt if we are not a source vertex for future epoch
         */
        if(!workerContext.isFutureLandmark(vertex.getId().get())){
            vertex.voteToHalt();
        }
    }

    private void setValue(DoubleWritable newValue,
                          Vertex<LongWritable, ArrayWritable, NullWritable> vertex, MSSPWorkerContext workerContext) {

        int currentEpoch = workerContext.getCurrentEpoch();

        ((DoubleWritable[]) vertex.getValue().get())[currentEpoch] = newValue;
    }

    public Double getCurrentEpochValue(Vertex<LongWritable, ArrayWritable, NullWritable> vertex,
                                       MSSPWorkerContext workerContext){

        int currentEpoch = workerContext.getCurrentEpoch();

        return ((DoubleWritable[]) vertex.getValue().get())[currentEpoch].get();
    }
}
