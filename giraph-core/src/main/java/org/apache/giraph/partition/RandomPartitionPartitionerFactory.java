package org.apache.giraph.partition;


import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class RandomPartitionPartitionerFactory<I extends WritableComparable,
        V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<I, V, E> {

    private int[] partitionToWorkerMapping;

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        super.setConf(conf);

         readWorkerToPartitionMapping(conf);
    }

    private void readWorkerToPartitionMapping(ImmutableClassesGiraphConfiguration<I, V, E> conf) {

        int numPartitions = conf.getUserPartitionCount();

        partitionToWorkerMapping = new int[numPartitions];

        String inputPathName = "partition.mapping";

        int lineNumber = 0;

        try {

            Path path = new Path(inputPathName);

            FileSystem fs = FileSystem.get(getConf());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            try {
                String line;

                while ((line = br.readLine()) != null){
                    this.partitionToWorkerMapping[lineNumber++] = Integer.parseInt(line);
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
    public int getPartition(I id, int partitionCount, int workerCount) {
        return Math.abs(id.hashCode() % partitionCount);
    }

    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return this.partitionToWorkerMapping[partition];
    }
}
