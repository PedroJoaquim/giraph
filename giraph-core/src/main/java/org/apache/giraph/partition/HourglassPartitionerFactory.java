package org.apache.giraph.partition;

import com.google.common.base.Charsets;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.regex.Pattern;

public class HourglassPartitionerFactory<V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {

    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    private Long2IntMap vertexToPartitionMapping;

    private Int2IntMap partitionToWorkerMapping;

    public HourglassPartitionerFactory() {
        readVertexToPartitionMapping();
    }

    private void readPartitionToWorkerMapping() {
        String line;

        BufferedReader reader = null;

        partitionToWorkerMapping = new Int2IntOpenHashMap();

        try {
            String partitionAssignmentPath = getConf().getPartitionAssignmentPath();

            DataInputStream in =
                    FileSystem.get(getConf()).open(new Path(partitionAssignmentPath));

            reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));

            while ((line = reader.readLine()) != null) {
                String[] split = SEPARATOR.split(line);
                partitionToWorkerMapping.put(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private void readVertexToPartitionMapping() {

        String line;

        BufferedReader reader = null;

        vertexToPartitionMapping = new Long2IntOpenHashMap();

        try {
            String vertexAssignmentPath = getConf().getVertexAssignmentPath();

            DataInputStream in =
                    FileSystem.get(getConf()).open(new Path(vertexAssignmentPath));

            reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));

            long id = 0;

            while ((line = reader.readLine()) != null) {
                vertexToPartitionMapping.put(Long.valueOf(id++), Integer.valueOf(line));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    @Override
    public int getPartition(LongWritable id, int partitionCount, int workerCount) {
        return this.vertexToPartitionMapping.get(id.get());
    }

    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return partition % workerCount;
    }
}
