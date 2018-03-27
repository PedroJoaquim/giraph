package org.apache.giraph.comm.requests;

import org.apache.giraph.master.MasterGlobalCommHandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AskForCheckpointSplitRequest extends WritableRequest
        implements MasterRequest {

    /** Task id of worker which requested the split */
    private int workerTaskId;

    public AskForCheckpointSplitRequest(int workerTaskId) {
        this.workerTaskId = workerTaskId;
    }

    @Override
    public void doRequest(MasterGlobalCommHandler commHandler) {

    }

    @Override
    public RequestType getType() {
        return null;
    }

    @Override
    void readFieldsRequest(DataInput input) throws IOException {

    }

    @Override
    void writeRequest(DataOutput output) throws IOException {

    }
}
