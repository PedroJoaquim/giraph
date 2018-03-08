package org.apache.giraph.emr;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class GiraphS3Checkpointer {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(GiraphS3Checkpointer.class);

    private static boolean first = true;

    private static String emrMasterIP;

    private static final String[] READ_MASTER_IP_CMD  = {
            "/bin/sh",
            "-c",
            "cat /mnt/var/lib/info/job-flow.json | jq -r '.masterPrivateDnsName'"
    };

    private static final String S3_DIST_CP_CMD_PREFIX = "s3-dist-cp --src /user/yarn/_bsp/_checkpoints/ --dest s3://inesc-giraph-emr//_bsp/_checkpoints/ --srcPattern=";

    public static void upload(long superstep, GiraphConfiguration giraphConf) throws IOException {

        String srcPattern = "'.*/" + superstep + "\\..*'";

        String s3Command =  S3_DIST_CP_CMD_PREFIX + srcPattern;

        String keyFileName = giraphConf.getEmrMasterKeysName() + ".pem";

        if(first){
            execProcess("hdfs dfs -get " + keyFileName, true, true, "hdfs");
            execProcess("chmod 400 " + keyFileName, true, true, "hdfs");
            emrMasterIP = readEmrMasterIp();
            first = false;
        }

        LOG.info("debug: key file = " + keyFileName);
        LOG.info("debug: emr master ip = " + emrMasterIP);
        LOG.info("debug: s3 cp cmd = " + s3Command);

        execProcess("ssh -o StrictHostKeyChecking=no -i " + keyFileName + " hadoop@" + emrMasterIP + " " + s3Command, false, true, "cmd");
    }

    private static String readEmrMasterIp() {

        String masterIP = "";

        try {
            Runtime rt = Runtime.getRuntime();

            Process p =rt. exec(READ_MASTER_IP_CMD);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            masterIP = stdInput.readLine();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return masterIP;
    }

    private static void execProcess(String cmd, boolean printResults, boolean waitProcess, String logRef){

        try {
            Runtime rt = Runtime.getRuntime();

            Process p = rt.exec(cmd);

            if(waitProcess){
                p.waitFor();
            }

            if(printResults){

                BufferedReader stdInput = new BufferedReader(new
                        InputStreamReader(p.getInputStream()));

                BufferedReader stdError = new BufferedReader(new
                        InputStreamReader(p.getErrorStream()));

                String s;

                while ((s = stdInput.readLine()) != null) {
                    LOG.info(logRef + "-debug-input: " + s);
                }

                // read any errors from the attempted command
                while ((s = stdError.readLine()) != null) {
                    LOG.info(logRef + "-debug-error: " + s);
                }
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
