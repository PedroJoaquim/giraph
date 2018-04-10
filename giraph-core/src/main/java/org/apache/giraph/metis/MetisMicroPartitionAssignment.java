package org.apache.giraph.metis;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetisMicroPartitionAssignment implements Writable {

    /**
     * Mapping from microPartitionId (index) to worker index(value)
     */
    private int[] microPartitionToWorkerMapping;

    /**
     * @param microPartitionToWorkerMapping
     */
    public MetisMicroPartitionAssignment(int[] microPartitionToWorkerMapping) {
        this.microPartitionToWorkerMapping = microPartitionToWorkerMapping;
    }

    public MetisMicroPartitionAssignment() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(this.microPartitionToWorkerMapping.length);

        for (int i = 0; i < this.microPartitionToWorkerMapping.length; i++) {
            dataOutput.writeInt(this.microPartitionToWorkerMapping[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        int numMicroPartitions = dataInput.readInt();

        this.microPartitionToWorkerMapping = new int[numMicroPartitions];

        for (int i = 0; i < numMicroPartitions; i++) {
            this.microPartitionToWorkerMapping[i] = dataInput.readInt();
        }
    }

    public void getExchangeInformation(int myWorkerIndex){
        //TODSO
    }

    public int[] getMicroPartitionToWorkerMapping() {
        return microPartitionToWorkerMapping;
    }
}
