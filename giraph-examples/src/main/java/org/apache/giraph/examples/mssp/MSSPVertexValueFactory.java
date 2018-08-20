package org.apache.giraph.examples.mssp;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.util.Arrays;

public class MSSPVertexValueFactory
        implements VertexValueFactory<MSSPVertexValue>, GiraphConfigurationSettable {

    private ImmutableClassesGiraphConfiguration conf;

    private static final DoubleWritable maxValue = new DoubleWritable(10000);

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration configuration) {
        this.conf = configuration;
    }

    @Override
    public MSSPVertexValue newInstance() {

        double[] initialValues = new double[MultipleSourcesShortestPaths.NUM_LANDMARKS.get(this.conf)];
        Arrays.fill(initialValues, maxValue.get());

        return new MSSPVertexValue(initialValues, initialValues.length);
    }
}
