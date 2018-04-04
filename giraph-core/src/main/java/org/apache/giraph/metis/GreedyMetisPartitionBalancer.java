package org.apache.giraph.metis;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

public class GreedyMetisPartitionBalancer<V extends Writable, E extends Writable> extends METISPartitionBalancer<LongWritable, V, E>{

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

        //TODO WRITE TO HDFS
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
            if(vertexAffinity[i] > maxAffinity && partitions.get(i).getVertexCount() < avgNumberOfVerticesPerPartition + 50){
                maxAffinity = vertexAffinity[i];
                maxAffinityIdx = i;
            }
        }

        partitions.get(maxAffinityIdx).putVertex(vertex);
        vertexAssignmentMap.put(vertex.getId().get(), maxAffinityIdx);
    }
}
