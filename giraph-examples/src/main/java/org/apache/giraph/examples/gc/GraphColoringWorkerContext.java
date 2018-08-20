package org.apache.giraph.examples.gc;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GraphColoringWorkerContext extends WorkerContext {

    private int currentState;

    private int currentColor;

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(GraphColoringWorkerContext.class);

    @Override
    public void preApplication() {

    }

    @Override
    public void postApplication() {

    }

    @Override
    public void preSuperstep() {

        if(getSuperstep() == 0){
            this.currentColor = 0;
            this.currentState = GraphColoringComputation.NO_STATE;
        }

        int previousState = this.currentState;

        if(previousState == GraphColoringComputation.NO_STATE){
            this.currentState = GraphColoringComputation.MIS_DEGREE_INIT_1;
        }
        else if(previousState == GraphColoringComputation.MIS_DEGREE_INIT_1){
            this.currentState = GraphColoringComputation.MIS_DEGREE_INIT_2;
        }
        else if(previousState == GraphColoringComputation.MIS_DEGREE_INIT_2){
            this.currentState = GraphColoringComputation.SELECTION;
        }
        else if(previousState == GraphColoringComputation.SELECTION){
            this.currentState = GraphColoringComputation.CONFLICT_RES;
        }
        else if(previousState == GraphColoringComputation.CONFLICT_RES){
            this.currentState = GraphColoringComputation.NOT_IN_S_DEGREE_ADJUST_1;
        }
        else if(previousState == GraphColoringComputation.NOT_IN_S_DEGREE_ADJUST_1){
            this.currentState = GraphColoringComputation.NOT_IN_S_DEGREE_ADJUST_2;
        }
        else if(previousState == GraphColoringComputation.NOT_IN_S_DEGREE_ADJUST_2){

            int unknownRemaining = this.<IntWritable>
                    getAggregatedValue(GraphColoringComputation.UNKNOWN_VERTICES_REMAINING).get();

            if(unknownRemaining > 0){
                //LOG.info(getSuperstep() + "-debug-gc: finished degree adjust, going back to selection - remaining: " + unknownRemaining);
                this.currentState = GraphColoringComputation.SELECTION;
            }
            else{
                //LOG.info(getSuperstep() + "-debug-gc: finished degree adjust, going to color assignment");
                this.currentState = GraphColoringComputation.COLOR_ASSIGNMENT;

                this.currentColor++;
            }
        }
        else if(previousState == GraphColoringComputation.COLOR_ASSIGNMENT){

            int unassignedRemaining = this.<IntWritable>
                    getAggregatedValue(GraphColoringComputation.UNASSIGNED_VERTICES_REMAINING).get();

            if(unassignedRemaining > 0){
                LOG.info(getSuperstep() + "-debug-gc: finished assignment round - remaining " + unassignedRemaining + " vertices to assign");
                this.currentState = GraphColoringComputation.MIS_DEGREE_INIT_1;
            }
            else {
                this.currentState = GraphColoringComputation.FINISHED;
                LOG.info(getSuperstep() + "-debug-gc: finished assignment round - finished");
            }
        }

        /*LOG.info(getSuperstep() + "-debug-gc: worker " + getMyWorkerIndex() +
                " | superstep: " + getSuperstep() +
                " | state selected = " + this.currentState);*/
    }

    @Override
    public void postSuperstep() {

    }

    public int getCurrentState() {
        return currentState;
    }

    public int getCurrentColor() {
        return currentColor;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(currentState);
        dataOutput.writeInt(currentColor);

        LOG.info("debug-gc-checkpoint: stored with currentState = " + currentState + ", currentColor = " + currentColor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        this.currentState = dataInput.readInt();
        this.currentColor = dataInput.readInt();

        LOG.info("debug-gc-checkpoint: loaded currentState = " + currentState + ", currentColor = " + currentColor);
    }
}
