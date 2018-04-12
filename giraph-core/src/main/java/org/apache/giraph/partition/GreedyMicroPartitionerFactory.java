package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.CheckpointingUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.*;

public class GreedyMicroPartitionerFactory<V extends Writable, E extends Writable>
        extends MicroPartitionerFactory<V, E> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(GreedyMicroPartitionerFactory.class);

    private int[] vertexToPartitionMapping;

    public GreedyMicroPartitionerFactory() {
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, V, E> conf) {
        super.setConf(conf);

        long start = System.currentTimeMillis();
        readVertexToPartitionMapping();
        long end = System.currentTimeMillis();

        LOG.info("debug-metis: time to load grredy vertex assignment = " + (end-start)/1000.0d + " secs");
    }

    public void readVertexToPartitionMapping() {

        int numGraphVertices = getConf().getNumGraphVertices();

        this.vertexToPartitionMapping = new int[numGraphVertices];

        String inputPathName = getConf().getVertexMappingPath();

        try {

            Path path = new Path(inputPathName);

            FileSystem fs = FileSystem.get(getConf());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            try {
                String line;
                int lineNumber = 0;
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
    public int getMicroPartition(LongWritable id){
        return vertexToPartitionMapping[(int) id.get()];
    }
}
