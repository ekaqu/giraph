package org.apache.giraph.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: dcapwell
 * Date: 11/10/11
 * Time: 6:40:41AM
 * To change this template use File | Settings | File Templates.
 */
public class TestUtil {
    private static final Log LOG = LogFactory.getLog(TestYarnClient.class);

    @Test
    public void testGetJarFile() {
        File jarFile = Util.getJarPath(ZooKeeper.class);

        LOG.info(jarFile);

        Assert.assertTrue("Jar file does not exist", jarFile.exists());
    }
}
