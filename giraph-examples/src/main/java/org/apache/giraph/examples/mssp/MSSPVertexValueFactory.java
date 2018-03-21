package org.apache.giraph.examples.mssp;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.util.Arrays;

public class MSSPVertexValueFactory
        implements VertexValueFactory<ArrayWritable<DoubleWritable>>, GiraphConfigurationSettable {

    private ImmutableClassesGiraphConfiguration conf;

    private static final DoubleWritable maxValue = new DoubleWritable(Double.MAX_VALUE);

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration configuration) {
        this.conf = configuration;
    }

    @Override
    public ArrayWritable<DoubleWritable> newInstance() {

        DoubleWritable[] initialValues = new DoubleWritable[MultipleSourcesShortestPaths.NUM_LANDMARKS.get(this.conf)];
        Arrays.fill(initialValues, maxValue);

        return new ArrayWritable<DoubleWritable>(DoubleWritable.class, initialValues);
    }
}
