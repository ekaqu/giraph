package org.apache.giraph.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;


public class TestYarnClient {
    private static final Log LOG = LogFactory.getLog(TestYarnClient.class);

    private static MiniDFSCluster dfsCluster;
    private static MiniYARNCluster yarnCluster;
    private static Configuration conf;

    @BeforeClass
    public static void init() throws IOException {
        conf = new Configuration();

        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

        yarnCluster = new MiniYARNCluster(TestYarnClient.class.getSimpleName());
        yarnCluster.init(conf);
        yarnCluster.start();
    }

    @AfterClass
    public static void cleanup() {
        yarnCluster.stop();
    }

    @Test
    public void testConect() throws IOException {
        YarnClient client = new YarnClient();
        client.init(conf);
    }

    @Test
    public void testStartAM() throws IOException {
        YarnClient client = new YarnClient();
        client.init(conf);

        boolean successful = client.waitForCompletion(true);
        Assert.assertTrue("Unable to launch AM", successful);
    }

    @Test
    public void testCreateStagingDir() throws IOException {
        Path stagingDir = YarnClient.getStagingDir(conf,
                BuilderUtils.newApplicationId(System.currentTimeMillis(), 10));

        LOG.info(stagingDir);

        Assert.assertEquals("hdfs", stagingDir.toUri().getScheme());
    }
}
