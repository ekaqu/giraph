package org.apache.giraph.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: dcapwell
 * Date: 11/9/11
 * Time: 8:36:45PM
 * To change this template use File | Settings | File Templates.
 */
public class GiraphApplicationMaster {
    private static final Log LOG = LogFactory.getLog(GiraphApplicationMaster.class.getName());

    public static void main(String[] args) {
        LOG.info("Launched Giraph App Master");

        Map<String, String> envs = System.getenv();
        LOG.info("Environment Variables");
        for(String key : envs.keySet()) {
            LOG.info(key + " => " + envs.get(key));
        }

        GiraphApplicationMaster appMaster = new GiraphApplicationMaster();
        appMaster.run();

        System.exit(0);
    }

    private Configuration conf;
    private YarnRPC rpc;

    // Handle to communicate with the Resource Manager
    private AMRMProtocol resourceManager;

    public GiraphApplicationMaster() {
        this.conf = new Configuration();
        this.conf.addResource("job.xml");
        rpc = YarnRPC.create(conf);

        ContainerId containerId = ConverterUtils.toContainerId(System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV));
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

        LOG.info("Application master for app"
        + ", appId=" + appAttemptID.getApplicationId().getId()
        + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());
    }

    public void run() {
        resourceManager = connectToRM();
    }


    /**
   * Connect to the Resource Manager
   * @return Handle to communicate with the RM
   */
  private AMRMProtocol connectToRM() {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
            YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
//    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
  }
}
