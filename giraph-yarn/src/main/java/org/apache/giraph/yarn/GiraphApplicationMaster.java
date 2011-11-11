package org.apache.giraph.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: dcapwell
 * Date: 11/9/11
 * Time: 8:36:45PM
 * To change this template use File | Settings | File Templates.
 */
public class GiraphApplicationMaster {
    private static final Log LOG = LogFactory.getLog(GiraphApplicationMaster.class.getName());

    public static void main(String[] args) throws YarnRemoteException {
        LOG.info("Launched Giraph App Master");

        Map<String, String> envs = System.getenv();
        LOG.info("Environment Variables");
        for (String key : envs.keySet()) {
            LOG.info(key + " => " + envs.get(key));
        }

        String nodeHttpPortString =
                System.getenv(ApplicationConstants.NM_HTTP_PORT_ENV);
        String appSubmitTimeStr =
                System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);

        ContainerId containerId = ConverterUtils.toContainerId(System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV));
        InetSocketAddress nodeManager = InetSocketAddress.createUnresolved(
                System.getenv(ApplicationConstants.NM_HOST_ENV),
                Integer.parseInt(System.getenv(ApplicationConstants.NM_PORT_ENV))
        );


        GiraphApplicationMaster appMaster = new GiraphApplicationMaster(nodeManager, containerId);

        System.exit(appMaster.run() ? 0 : 1);
    }

    private Configuration conf;
    private YarnRPC rpc;
    private InetSocketAddress nodeManager;
    private ApplicationAttemptId appAttemptID;

    // Handle to communicate with the Resource Manager
    private AMRMProtocol resourceManager;

    public GiraphApplicationMaster(InetSocketAddress nodeManager, ContainerId containerId) {
        this.conf = new Configuration();
        this.conf.addResource("job.xml");
        rpc = YarnRPC.create(conf);

        this.nodeManager = nodeManager;
        appAttemptID = containerId.getApplicationAttemptId();

        if (LOG.isInfoEnabled()) {
            LOG.info("Application master for app"
                    + ", appId=" + appAttemptID.getApplicationId().getId()
                    + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
                    + ", attemptId=" + appAttemptID.getAttemptId());
        }

        resourceManager = connectToRM();
    }

    /**
     * Register the Application Master to the Resource Manager
     *
     * @return the registration response from the RM
     * @throws org.apache.hadoop.yarn.exceptions.YarnRemoteException
     *
     */
    private RegisterApplicationMasterResponse registerToRM() throws YarnRemoteException {
        RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);

        // set the required info into the registration request:
        // application attempt id,
        // host on which the app master is running
        // rpc port on which the app master accepts requests from the client
        // tracking url for the app master
        appMasterRequest.setApplicationAttemptId(appAttemptID);
        //TODO implement Rpc interface for AM
        appMasterRequest.setHost(nodeManager.getHostName());
        appMasterRequest.setRpcPort(0);
        appMasterRequest.setTrackingUrl("");

        return resourceManager.registerApplicationMaster(appMasterRequest);
    }

    public boolean run() throws YarnRemoteException {
        // register the AM with the RM
        RegisterApplicationMasterResponse response = registerToRM();

        // Dump out information about cluster capability as seen by the resource manager
        int minMem = response.getMinimumResourceCapability().getMemory();
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Min mem capabililty of resources in this cluster " + minMem);
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // launch containers

        // When the application completes, it should send a finish application signal
        // to the RM
        FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
        finishReq.setAppAttemptId(appAttemptID);
        boolean isSuccess = true;
        if (0 == 0) { //TODO add real logic here to find out how many containers failed
          finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        }
        else {
          finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
          String diagnostics = "Diagnostics."
//              + ", total=" + numTotalContainers
//              + ", completed=" + numCompletedContainers.get()
//              + ", allocated=" + numAllocatedContainers.get()
//              + ", failed=" + numFailedContainers.get();
            + "total=0, completed=0, allocated=0, failed=0";
          finishReq.setDiagnostics(diagnostics);
          isSuccess = false;
        }

        LOG.info("Application completed with status "+finishReq.getFinalApplicationStatus()+". Signalling finish to RM");
        resourceManager.finishApplicationMaster(finishReq);
        return isSuccess;
    }


    /**
     * Connect to the Resource Manager
     *
     * @return Handle to communicate with the RM
     */
    private AMRMProtocol connectToRM() {
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
                YarnConfiguration.RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
        LOG.info("Connecting to ResourceManager at " + rmAddress);
        return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
    }
}
