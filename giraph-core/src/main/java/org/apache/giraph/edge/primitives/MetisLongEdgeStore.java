package org.apache.giraph.edge.primitives;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.partition.MicroPartitionerFactory;
import org.apache.giraph.utils.VertexIdEdgeIterator;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetisLongEdgeStore<V extends Writable, E extends Writable> extends LongEdgeStore<V, E> {

    //store info about the edges going to other partitions
    private ConcurrentHashMap<Integer, Int2LongOpenHashMap> outgoingEdgesInfo;

    private CentralizedServiceWorker<LongWritable, V, E> service;

    private MicroPartitionerFactory<V, E> microPartitionerFactory;

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(MetisLongEdgeStore.class);

    /**
     * Constructor.
     *
     * @param service       Service worker
     * @param configuration Configuration
     * @param progressable  Progressable
     */
    public MetisLongEdgeStore(CentralizedServiceWorker<LongWritable, V, E> service,
                              ImmutableClassesGiraphConfiguration<LongWritable, V, E> configuration,
                              Progressable progressable,
                              MicroPartitionerFactory<V, E> microPartitionerFactory) {

        super(service, configuration, progressable);

        this.service = service;
        this.outgoingEdgesInfo = new ConcurrentHashMap<>();
        this.microPartitionerFactory = microPartitionerFactory;
    }

    @Override
    public void addPartitionEdges(
            int partitionId, VertexIdEdges<LongWritable, E> edges) {

        Map<Long, OutEdges<LongWritable, E>> partitionEdges = getPartitionEdges(partitionId);

        VertexIdEdgeIterator<LongWritable, E> vertexIdEdgeIterator =
                edges.getVertexIdEdgeIterator();

        while (vertexIdEdgeIterator.hasNext()) {

            vertexIdEdgeIterator.next();

            Edge<LongWritable, E> edge = reuseEdgeObjects ?
                    vertexIdEdgeIterator.getCurrentEdge() :
                    vertexIdEdgeIterator.releaseCurrentEdge();

            OutEdges<LongWritable, E> outEdges = getVertexOutEdges(vertexIdEdgeIterator,
                    partitionEdges);

            int microPartitionId = this.microPartitionerFactory.getMicroPartition(vertexIdEdgeIterator.getCurrentVertexId());

            Int2LongOpenHashMap partitionMap = this.outgoingEdgesInfo.get(microPartitionId);

            if(partitionMap == null){
                partitionMap = new Int2LongOpenHashMap();
                partitionMap.defaultReturnValue(0);
                if(this.outgoingEdgesInfo.putIfAbsent(microPartitionId, partitionMap) != null){
                    partitionMap = this.outgoingEdgesInfo.get(microPartitionId);
                }
            }

            int targetMicroPartitionId = this.microPartitionerFactory.getMicroPartition(edge.getTargetVertexId());

            synchronized (partitionMap){
                long previousValue = partitionMap.get(targetMicroPartitionId);
                partitionMap.put(targetMicroPartitionId, (previousValue + 1));
            }

            synchronized (outEdges) {
                outEdges.add(edge);
            }
        }
    }

    public Int2LongOpenHashMap getEdgesFromPartition(int from){
        return this.outgoingEdgesInfo.get(from);
    }

    public int numPartitions(){
        return this.outgoingEdgesInfo.size();
    }

    public void reset(){
        this.outgoingEdgesInfo = null;
    }
}
