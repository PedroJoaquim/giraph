package org.apache.giraph.emr.s3;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class S3Checkpointer extends S3Com{

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(S3Checkpointer.class);

    private static boolean first = true;

    private static String emrMasterIP;

    private static String MASTER_IP_JSON_ENTRY_NAME = "masterPrivateDnsName";

    private static final String S3_DIST_CP_CMD_PREFIX = "s3-dist-cp --src /user/yarn/_bsp/_checkpoints/ --dest s3://inesc-giraph-emr//_bsp/_checkpoints/ --srcPattern=";

    public static void upload(long superstep, GiraphConfiguration giraphConf) throws IOException {

        final String srcPattern = "'.*/" + superstep + "\\..*'";

        final String s3Command =  S3_DIST_CP_CMD_PREFIX + srcPattern;

        final String key = downloadKeyFile(giraphConf);

        if(first){
            emrMasterIP = readClusterInfo(MASTER_IP_JSON_ENTRY_NAME);
            first = false;
        }

        LOG.info("s3-checkpoint-upload: key file = " + key);
        LOG.info("s3-checkpoint-upload: emr master ip = " + emrMasterIP);
        LOG.info("s3-checkpoint-upload: s3 cp cmd = " + s3Command);

        Thread t = new Thread() {
            public void run() {
                long start = System.currentTimeMillis();
                execProcess("ssh -o StrictHostKeyChecking=no -i " + key + " hadoop@" + emrMasterIP + " " + s3Command, false, true, "cmd");
                long end = System.currentTimeMillis();

                LOG.info("analysis-checkpoint-s3: time = " + (end - start)/1000 + " seconds");
            }
        };

        t.start();
    }
}
