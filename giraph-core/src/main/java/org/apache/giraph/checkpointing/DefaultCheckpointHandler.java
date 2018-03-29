package org.apache.giraph.checkpointing;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.checkpointing.path.DefaultCheckpointPathManager;
import org.apache.giraph.master.checkpointing.DefaultMasterCheckpointHandler;
import org.apache.giraph.worker.checkpointing.DefaultWorkerCheckpointHandler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DefaultCheckpointHandler
        <I extends WritableComparable,
                V extends Writable,
                E extends Writable> implements CheckpointHandler<I, V, E>{

    @Override
    public WorkerCheckpointHandler<I, V, E> createWorkerCheckpointHandler(
            CentralizedServiceWorker<I, V, E> centralizedServiceWorker,
            BspService<I, V, E> bspService,
            CheckpointPathManager pathManager) {

        WorkerCheckpointHandler<I, V, E> workerCheckpointHandler = new DefaultWorkerCheckpointHandler<I, V, E>();
        workerCheckpointHandler.initialize(centralizedServiceWorker, bspService, pathManager);

        return workerCheckpointHandler;
    }

    @Override
    public MasterCheckpointHandler<I, V, E> createMasterCheckpointHandler(
            CentralizedServiceMaster<I, V, E> centralizedServiceMaster,
            BspService<I, V, E> bspService,
            CheckpointPathManager pathManager) {

        MasterCheckpointHandler<I, V, E> masterCheckpointHandler = new DefaultMasterCheckpointHandler<I, V, E>();
        masterCheckpointHandler.initialize(centralizedServiceMaster, bspService, pathManager);

        return masterCheckpointHandler;
    }

    @Override
    public CheckpointPathManager createPathManager(
            String savedCheckpointBasePath,
            String checkpointBasePath) {

        return new DefaultCheckpointPathManager(savedCheckpointBasePath, checkpointBasePath);
    }
}
