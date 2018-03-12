package org.apache.giraph.emr.s3;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class S3InfoSender extends S3Com {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(S3InfoSender.class);

    private static String UPLOAD_TMP_FILE_NAME = "/tmp/giraph_info.txt";

    private static String CLUSTER_NAME_JSON_NAME = "jobFlowId";

    private static String S3_BUCKET_URL = "s3://inesc-giraph-emr/info/%s/giraph_cmd.txt";

    public static void uploadInfoToS3(double setupSecs, Map<Long, Double> superstepSecsMap, double shutdownSecs, double totalSecs) {

        final String clusterID = readClusterInfo(CLUSTER_NAME_JSON_NAME);

        writeTmpFile(setupSecs, superstepSecsMap, shutdownSecs, totalSecs);

        uploadToS3(clusterID);
    }

    private static void uploadToS3(String clusterID) {

        String s3URL = String.format(S3_BUCKET_URL, clusterID);

        String cmd = "aws s3 cp " +
                UPLOAD_TMP_FILE_NAME +
                " " +
                s3URL;

        LOG.info("s3-upload-cmd = " + cmd);

        execProcess(cmd, true, true, "s3-info-upload");
    }

    private static void writeTmpFile(double setupSecs, Map<Long, Double> superstepSecsMap, double shutdownSecs, double totalSecs) {

        PrintWriter writer = null;

        try {
            writer = new PrintWriter(UPLOAD_TMP_FILE_NAME, "UTF-8");

            writer.println(setupSecs);

            for (Map.Entry<Long, Double> entry : superstepSecsMap.entrySet()) {
                writer.println(entry.getKey() + "#" + entry.getValue());
            }

            writer.println(shutdownSecs);
            writer.println(totalSecs);

            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }
}