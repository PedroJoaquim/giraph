package org.apache.giraph.emr.s3;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.log4j.Logger;
import org.python.antlr.ast.Str;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class S3Com {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(S3Com.class);

    private static boolean downloadKeyPair = false;

    private static String keyFileName;

    private static final String[] CLUSTER_INFO_CMD  = {
            "/bin/sh",
            "-c",
            "cat /mnt/var/lib/info/job-flow.json | jq -r '.%s'"
    };

    protected static String downloadKeyFile(GiraphConfiguration giraphConf){

        if(!downloadKeyPair){

            keyFileName =  giraphConf.getEmrMasterKeysName() + ".pem";
            execProcess("hdfs dfs -get " + keyFileName, false, true, "hdfs");
            execProcess("chmod 400 " + keyFileName, false, true, "hdfs");

            downloadKeyPair = true;
        }

        return keyFileName;
    }

    protected static String readClusterInfo(String jsonEntryName){

        String targetInfo = "";

        CLUSTER_INFO_CMD[2] = String.format(CLUSTER_INFO_CMD[2], jsonEntryName);

        try {
            Runtime rt = Runtime.getRuntime();

            Process p = rt.exec(CLUSTER_INFO_CMD);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            targetInfo = stdInput.readLine();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return targetInfo;
    }

    protected static void execProcess(String cmd, boolean printResults, boolean waitProcess, String logRef){

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
                    LOG.info(logRef + "-input: " + s);
                }

                // read any errors from the attempted command
                while ((s = stdError.readLine()) != null) {
                    LOG.info(logRef + "-error: " + s);
                }
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
