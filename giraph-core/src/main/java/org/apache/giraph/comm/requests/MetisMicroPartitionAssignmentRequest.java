package org.apache.giraph.comm.requests;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.metis.MetisMicroPartitionAssignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetisMicroPartitionAssignmentRequest extends WritableRequest
        implements WorkerRequest {

    private MetisMicroPartitionAssignment metisMicroPartitionAssignment;

    /** Constructor for reflection */
    public MetisMicroPartitionAssignmentRequest() {
    }

    public MetisMicroPartitionAssignmentRequest(MetisMicroPartitionAssignment metisMicroPartitionAssignment) {
        this.metisMicroPartitionAssignment = metisMicroPartitionAssignment;
    }

    @Override
    public void doRequest(ServerData serverData) {
        serverData.getServiceWorker().metisMicroPartitionAssignmentReceived(this.metisMicroPartitionAssignment);
    }

    @Override
    public RequestType getType() {
        return RequestType.METIS_MICRO_PARTITION_ASSIGNMENT_REQUEST;
    }

    @Override
    void readFieldsRequest(DataInput input) throws IOException {
        metisMicroPartitionAssignment = new MetisMicroPartitionAssignment();
        metisMicroPartitionAssignment.readFields(input);
    }

    @Override
    void writeRequest(DataOutput output) throws IOException {
        this.metisMicroPartitionAssignment.write(output);
    }
}
