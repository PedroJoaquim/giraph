package org.apache.giraph.checkpointing;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface CheckpointHandler
        <I extends WritableComparable,
        V extends Writable,
        E extends Writable> {


    WorkerCheckpointHandler<I, V, E> createWorkerCheckpointHandler(
            CentralizedServiceWorker<I, V, E> centralizedServiceWorker,
            BspService<I, V, E> bspService,
            CheckpointPathManager pathManager);

    MasterCheckpointHandler<I, V, E> createMasterCheckpointHandler(
            CentralizedServiceMaster<I, V, E> centralizedServiceMaster,
            BspService<I, V, E> bspService,
                CheckpointPathManager pathManager);

    CheckpointPathManager createPathManager(
            String savedCheckpointBasePath,
            String checkpointBasePath);

}
