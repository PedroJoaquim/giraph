package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class RandomVertexPartitionerFactory<
        V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<LongWritable, V, E> {

    private int[] vertexToPartitionMapping;

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, V, E> conf) {
        super.setConf(conf);

        readVertexToPartitionMapping(conf);
    }

    private void readVertexToPartitionMapping(ImmutableClassesGiraphConfiguration<LongWritable, V, E> conf) {

        int numVertices = conf.getNumGraphVertices();

        vertexToPartitionMapping = new int[numVertices];

        String inputPathName = "vertex.mapping";

        int lineNumber = 0;

        try {

            Path path = new Path(inputPathName);

            FileSystem fs = FileSystem.get(getConf());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            try {
                String line;

                while ((line = br.readLine()) != null){
                    this.vertexToPartitionMapping[lineNumber++] = Integer.parseInt(line);
                }

            } finally {
                br.close();
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getPartition(LongWritable id, int partitionCount, int workerCount) {
        return this.vertexToPartitionMapping[(int) id.get()];
    }

    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return partition % workerCount;
    }
}
