package org.apache.giraph.examples.gc;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Random;

public class GraphColoringComputation
        extends BasicComputation<LongWritable,
        GraphColoringVertexValue,
        NullWritable,
        GraphColoringMessage> {

    protected static final String CURRENT_PHASE_AGGREGATOR_NAME = "phase-aggregator";

    protected static final String UNKNOWN_VERTICES_REMAINING = "unknown-remaining";

    protected static final String UNASSIGNED_VERTICES_REMAINING = "unassigned-remaining";

    private static final Random rnd = new Random();

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(GraphColoringComputation.class);

    /* ALGORITHM STATE */

    protected static final int NO_STATE = -1;

    protected static final int MIS_DEGREE_INIT_1 = 0;

    protected static final int MIS_DEGREE_INIT_2 = 1;

    protected static final int SELECTION = 2;

    protected static final int CONFLICT_RES = 3;

    protected static final int NOT_IN_S_DEGREE_ADJUST_1 = 4;

    protected static final int NOT_IN_S_DEGREE_ADJUST_2 = 5;

    protected static final int COLOR_ASSIGNMENT = 6;

    protected static final int FINISHED = 7;

    /* COLOR */

    protected static final int UNDEFINED_COLOR = -1;

    /* TYPE */

    private static final int UNKNOWN_TYPE = 1;

    private static final int TENTATIVELY_IN_S_TYPE = 2;

    private static final int IN_S_TYPE = 3;

    private static final int NOT_IN_S_TYPE = 4;

    @Override
    public void compute(Vertex<LongWritable, GraphColoringVertexValue, NullWritable> vertex,
                        Iterable<GraphColoringMessage> messages) throws IOException {

        if(getSuperstep() == 0){
            vertex.setValue(new GraphColoringVertexValue());
            vertex.getValue().setColor(UNDEFINED_COLOR);
        }

        int currentState = this.<GraphColoringWorkerContext>getWorkerContext().getCurrentState();

        if(vertex.getValue().getColor() != UNDEFINED_COLOR || currentState == FINISHED){
            vertex.voteToHalt();
        }
        else if(currentState == MIS_DEGREE_INIT_1){

            vertex.getValue().setType(UNKNOWN_TYPE);

            sendMessageToAllEdges(vertex, new GraphColoringMessage(vertex.getId().get()));
        }
        else if(currentState == MIS_DEGREE_INIT_2){

            int numMessages = 0;

            for (GraphColoringMessage msg : messages){
                numMessages++;
            }

            //LOG.info(getSuperstep() + "-debug-gc-mis-degree: vertex " + vertex.getId().get() + " degree = " + numMessages);

            vertex.getValue().setDegree(numMessages);
        }
        else if(currentState == SELECTION){

            if(vertex.getValue().getType() == UNKNOWN_TYPE){

                double prob;

                int unknownRemaining = this.<IntWritable>
                        getAggregatedValue(GraphColoringComputation.UNKNOWN_VERTICES_REMAINING).get();

                if(vertex.getValue().getDegree() <= 0 || (unknownRemaining > 0 && unknownRemaining <= 10)){
                    prob = 1.0;
                }
                else {
                    prob = 1.0d / (2 * vertex.getValue().getDegree());
                }

                //LOG.info(getSuperstep() + "-debug-gc: vertex " + vertex.getId().get() + " prob = " + probInt);

                if(rnd.nextDouble() <=  prob){
                    vertex.getValue().setType(TENTATIVELY_IN_S_TYPE);
                    //LOG.info(getSuperstep() + "-debug-gc-selection: vertex " + vertex.getId().get() + " temptive in S ");
                    sendMessageToAllEdges(vertex, new GraphColoringMessage(vertex.getId().get()));
                }
            }
        }
        else if(currentState == CONFLICT_RES){

            long minId = Long.MAX_VALUE;

            if(vertex.getValue().getType() == TENTATIVELY_IN_S_TYPE){
                for (GraphColoringMessage msg : messages) {

                    //LOG.info(getSuperstep() + "-debug-gc-conflict-rest: vertex " + vertex.getId().get() + " received from: " + msg.getVertexId());

                    if(msg.getVertexId() < minId){

                        minId = msg.getVertexId();
                    }
                }

                if(vertex.getId().get() < minId){
                    vertex.getValue().setType(IN_S_TYPE);
                    //LOG.info(getSuperstep() + "-debug-gc-conflict-rest: vertex " + vertex.getId().get() + " IN S ");
                    sendMessageToAllEdges(vertex, new GraphColoringMessage(vertex.getId().get()));
                }
                else {
                    vertex.getValue().setType(UNKNOWN_TYPE);
                }
            }
        }
        else if(currentState == NOT_IN_S_DEGREE_ADJUST_1){

            int numMessages = 0;

            for (GraphColoringMessage msg : messages){
                numMessages++;
            }

            if(numMessages > 0 && vertex.getValue().getType() == IN_S_TYPE){
                LOG.info(getSuperstep() + "-debug-gc-error: received " + numMessages + " messages from neighbors while in set (" + vertex.getId() + ")");
            }
            else if(numMessages > 0 && vertex.getValue().getType() == UNKNOWN_TYPE) {
                vertex.getValue().setType(NOT_IN_S_TYPE);

                sendMessageToAllEdges(vertex, new GraphColoringMessage(vertex.getId().get()));
            }
        }
        else if(currentState == NOT_IN_S_DEGREE_ADJUST_2){

            if(vertex.getValue().getType() == UNKNOWN_TYPE){
                int previousDegree = vertex.getValue().getDegree();

                int numMessages = 0;

                for (GraphColoringMessage msg : messages){
                    numMessages++;
                }

                vertex.getValue().setDegree(previousDegree - numMessages);

                if(previousDegree - numMessages < 0){
                    LOG.info(getSuperstep() + "-debug-gc-error: new degree < 0!");
                }

                aggregate(UNKNOWN_VERTICES_REMAINING, new IntWritable(1));
            }
        }
        else if(currentState == COLOR_ASSIGNMENT){

            if(vertex.getValue().getType() == IN_S_TYPE){
                int currentColor = this.<GraphColoringWorkerContext>getWorkerContext().getCurrentColor();
                //LOG.info(getSuperstep() + "-debug-gc-color-assignment: vertex " + vertex.getId().get() + " assigning color " + currentColor);
                vertex.getValue().setColor(currentColor);
            }
            else {
                //LOG.info(getSuperstep() + "-debug-gc-color-assignment: vertex " + vertex.getId().get() + " going back to unknown");
                aggregate(UNASSIGNED_VERTICES_REMAINING, new IntWritable(1));
            }
        }
    }
}
