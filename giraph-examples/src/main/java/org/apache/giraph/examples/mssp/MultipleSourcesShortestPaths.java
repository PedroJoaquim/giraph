package org.apache.giraph.examples.mssp;

import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.examples.Algorithm;

@Algorithm(
        name = "Multiple Source Shortest paths",
        description = "Finds all shortest paths from a selected set of vertex"
)

public class MultipleSourcesShortestPaths {

    public static final String LANDMARK_VERTICES_AGG = "landmarks";

    public static final String MESSAGES_SENT_AGG = "messages";

    public static final String CURRENT_EPOCH_AGG = "epoch";

    /** The shortest paths id */
    public static final IntConfOption NUM_LANDMARKS =
            new IntConfOption("MultipleShortestPaths.num.landmarks", 30,
                    "number of landmarks");


    public static final String AGGREGATOR_SEPARATOR = "#";


}
