package org.apache.giraph.examples.mssp;

import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

@Algorithm(
        name = "Multiple Source Shortest paths",
        description = "Finds all shortest paths from a selected set of vertex"
)

public class MultipleSourcesShortestPaths
        extends BasicComputation<LongWritable, ArrayWritable<DoubleWritable>, NullWritable, DoubleWritable>{

    public static final String LANDMARK_VERTICES_AGG = "landmarks";

    public static final String MESSAGES_SENT_AGG = "messages";

    public static final String CURRENT_EPOCH_AGG = "epoch";

    /** Number of landmarks to compute */
    public static final IntConfOption NUM_LANDMARKS =
            new IntConfOption("MultipleShortestPaths.numLandmarks", 30,
                    "number of landmarks");

    /** ing on landmark id */
    public static final IntConfOption INC_LANDMARKS =
            new IntConfOption("MultipleShortestPaths.incLandmarks", 100,
                    "inc for landmark id");

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(MultipleSourcesShortestPaths.class);


    public static final String AGGREGATOR_SEPARATOR = "#";

    @Override
    public void compute(Vertex<LongWritable, ArrayWritable<DoubleWritable>, NullWritable> vertex, Iterable<DoubleWritable> messages)
            throws IOException {


        MSSPWorkerContext workerContext = this.<MSSPWorkerContext>getWorkerContext();

        if(LOG.isDebugEnabled()){
            LOG.debug("debug-mssp: vertex = " + vertex.getId().get() + " superstep = " + getSuperstep());
            LOG.debug("debug-mssp: current epoch = " + workerContext.getCurrentEpoch() + " is source = " + workerContext.isSourceVertexForCurrentEpoch(vertex.getId().get()));
            LOG.debug("debug-mssp: current epoch value = " + getCurrentEpochValue(vertex, workerContext));
        }
        
        double minDist =
                workerContext.isSourceVertexForCurrentEpoch(vertex.getId().get()) ? 0d : Double.MAX_VALUE;

        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }

        if (minDist < getCurrentEpochValue(vertex, workerContext)) {
            setValue(new DoubleWritable(minDist), vertex, workerContext);

            double distance = minDist + 1;
            sendMessageToAllEdges(vertex, new DoubleWritable(distance));

            aggregate(MESSAGES_SENT_AGG, new LongWritable(vertex.getNumEdges()));
        }

        voteToHaltIfNecessary(vertex, workerContext);
    }

    private void voteToHaltIfNecessary(Vertex<LongWritable, ArrayWritable<DoubleWritable>, NullWritable> vertex,
                                       MSSPWorkerContext workerContext) {

        /*
         * We only vote to halt if we are not a source vertex for future epoch
         */
        if(!workerContext.isFutureLandmark(vertex.getId().get())){
            vertex.voteToHalt();
        }
    }

    private void setValue(DoubleWritable newValue,
                          Vertex<LongWritable, ArrayWritable<DoubleWritable>, NullWritable> vertex, MSSPWorkerContext workerContext) {

        int currentEpoch = workerContext.getCurrentEpoch();

        vertex.getValue().get()[currentEpoch] = newValue;
    }

    private Double getCurrentEpochValue(Vertex<LongWritable, ArrayWritable<DoubleWritable>, NullWritable> vertex,
                                       MSSPWorkerContext workerContext){

        int currentEpoch = workerContext.getCurrentEpoch();

        return vertex.getValue().get()[currentEpoch].get();
    }
}
