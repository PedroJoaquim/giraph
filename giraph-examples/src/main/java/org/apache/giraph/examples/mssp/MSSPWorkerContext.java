package org.apache.giraph.examples.mssp;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

public class MSSPWorkerContext extends WorkerContext {

    private HashSet<Long> futureLandmarks;

    private Long[] landmarksArray;

    private int currentEpoch;

    private static final Logger LOG =
            Logger.getLogger(MSSPWorkerContext.class);

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {

        Text aggregatedValue = this.<Text>getAggregatedValue(MultipleSourcesShortestPaths.LANDMARK_VERTICES_AGG);

        LOG.info("debug-mssp: aggregatedValue = " + aggregatedValue);

        String[] splited = aggregatedValue.toString().split(MultipleSourcesShortestPaths.AGGREGATOR_SEPARATOR);

        this.currentEpoch = 0;

        this.landmarksArray = new Long[splited.length];

        this.futureLandmarks = new HashSet<Long>();

        for (int i = 0; i < splited.length; i++) {
            this.landmarksArray[i] = Long.parseLong(splited[i]);

            if(i > 0){
                this.futureLandmarks.add(this.landmarksArray[i]);
            }
        }
    }

    protected boolean isFutureLandmark(Long vertexID){
        return this.futureLandmarks.contains(vertexID);
    }

    protected boolean isSourceVertexForCurrentEpoch(Long vertexID){
        return this.landmarksArray[this.currentEpoch].equals(vertexID);
    }

    protected int getCurrentEpoch(){
        return this.currentEpoch;
    }

    protected int getNumLandmarks(){
        return this.landmarksArray.length;
    }

    @Override
    public void postApplication() {

    }

    @Override
    public void preSuperstep() {

        int newEpoch =  this.<IntWritable>getAggregatedValue(MultipleSourcesShortestPaths.CURRENT_EPOCH_AGG).get();

        if(newEpoch != this.currentEpoch){

            LOG.info("debug-mssp: new epoch " + newEpoch + " at superstep: " + getSuperstep());
            
            this.currentEpoch = newEpoch;

            this.futureLandmarks = new HashSet<Long>();

            for (int i = newEpoch + 1; i < this.landmarksArray.length; i++) {
                this.futureLandmarks.add(this.landmarksArray[i]);
            }
        }
    }

    @Override
    public void postSuperstep() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //TODO
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //TODO
    }
}
