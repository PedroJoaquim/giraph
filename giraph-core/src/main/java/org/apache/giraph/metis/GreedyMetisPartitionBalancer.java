package org.apache.giraph.metis;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.*;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GreedyMetisPartitionBalancer<V extends Writable, E extends Writable> extends METISPartitionBalancer<LongWritable, V, E>{

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(GreedyMetisPartitionBalancer.class);
    @Override
    public void reassignPartitions(BspServiceWorker<LongWritable, V, E> serviceWorker) {


        List<Partition<LongWritable, V, E>> newPartitionList = new ArrayList<>();

        Long2IntOpenHashMap vertexAssignmentMap = new Long2IntOpenHashMap();
        vertexAssignmentMap.defaultReturnValue(-1);

        for(PartitionOwner po : serviceWorker.getPartitionOwners()){
            if(po.getWorkerInfo().getTaskId() == serviceWorker.getWorkerInfo().getTaskId()){
                newPartitionList.add(serviceWorker.getConfiguration().createPartition(po.getPartitionId(), serviceWorker.getContext()));
            }
        }

        PartitionStore<LongWritable, V, E> partitionStore = serviceWorker.getServerData().getPartitionStore();

        partitionStore.startIteration();

        long verticesPerPartition = 0;

        while (true){

            Partition<LongWritable, V, E> oldPartition =
                    partitionStore.getNextPartition();

            if (oldPartition == null) {
                break;
            }

            if(verticesPerPartition == 0){
                verticesPerPartition = oldPartition.getVertexCount();
            }

            for (Vertex<LongWritable, V, E> vertex : oldPartition){
                 assignToPartition(vertex, vertexAssignmentMap, newPartitionList, verticesPerPartition);
            }
        }

        for (Partition<LongWritable, V, E> newPartition : newPartitionList) {
            partitionStore.removePartition(newPartition.getId());
            partitionStore.addPartition(newPartition);
        }

        writeMappingToHDFS(newPartitionList, serviceWorker);
    }

    private void writeMappingToHDFS(List<Partition<LongWritable, V, E>> newPartitions, final BspServiceWorker<LongWritable, V, E> serviceWorker) {

        int numThreads = Math.min(
                GiraphConstants.NUM_CHECKPOINT_IO_THREADS.get(serviceWorker.getConfiguration()),
                newPartitions.size());


        final Queue<Partition<LongWritable, V, E>> partitionQueue =
                new ConcurrentLinkedQueue<>(newPartitions);


        final FileSystem fs = serviceWorker.getFs();

        CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
            @Override
            public Callable<Void> newCallable(int callableId) {
                return new Callable<Void>() {
                    @Override
                    public Void call() {

                        while (!partitionQueue.isEmpty()) {

                            Partition<LongWritable, V, E> partition = partitionQueue.poll();

                            if (partition == null) {
                                break;
                            }

                            String path = serviceWorker.getConfiguration().getVertexAssignmentPath() + partition.getId() + ".mapping";

                            try {
                                FSDataOutputStream fileStream =
                                        fs.create(new Path(path));


                                fileStream.writeInt(partition.getId());
                                fileStream.writeLong(partition.getVertexCount());

                                long acc = 0;

                                for (Vertex<LongWritable, V, E> v : partition) {
                                    fileStream.writeLong(v.getId().get());
                                    acc++;
                                }

                                if(acc != partition.getVertexCount()) LOG.info("debug-metis: acc = " + acc + " vertex num = " +  partition.getVertexCount());

                                fileStream.close();

                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        }

                        return null;
                    }
                };
            }
        };

        ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
                "metis-read-%d", serviceWorker.getContext());

    }

    private void assignToPartition(Vertex<LongWritable, V, E> vertex,
                                           Long2IntOpenHashMap vertexAssignmentMap,
                                           List<Partition<LongWritable, V, E>> partitions,
                                           long avgNumberOfVerticesPerPartition) {

        int maxAffinity = -1;
        int maxAffinityIdx = -1;

        int[] vertexAffinity = new int[partitions.size()];

        for(Edge<LongWritable, E> edge : vertex.getEdges()){

            int assignedPartitionId = vertexAssignmentMap.get(edge.getTargetVertexId().get());

            if(assignedPartitionId >= 0){
                vertexAffinity[assignedPartitionId]++;
            }
        }

        for (int i = 0; i < partitions.size(); i++) {
            if(vertexAffinity[i] > maxAffinity && partitions.get(i).getVertexCount() < avgNumberOfVerticesPerPartition + 10){
                maxAffinity = vertexAffinity[i];
                maxAffinityIdx = i;
            }
        }

        partitions.get(maxAffinityIdx).putVertex(vertex);
        vertexAssignmentMap.put(vertex.getId().get(), maxAffinityIdx);
    }
}