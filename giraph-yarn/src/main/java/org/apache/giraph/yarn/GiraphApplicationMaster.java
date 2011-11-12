package org.apache.giraph.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import javax.management.RuntimeErrorException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: dcapwell
 * Date: 11/9/11
 * Time: 8:36:45PM
 * To change this template use File | Settings | File Templates.
 */
//TODO refactor the code to make it more modular
public class GiraphApplicationMaster {
    private static final Log LOG = LogFactory.getLog(GiraphApplicationMaster.class.getName());
    private int containerMemory;

    private AtomicInteger rmRequestID = new AtomicInteger();
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();


    public static void main(String[] args) throws YarnRemoteException {
        LOG.info("Launched Giraph App Master");

        ContainerId containerId = ConverterUtils.toContainerId(System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV));
        InetSocketAddress nodeManager = InetSocketAddress.createUnresolved(
                System.getenv(ApplicationConstants.NM_HOST_ENV),
                Integer.parseInt(System.getenv(ApplicationConstants.NM_PORT_ENV))
        );

        GiraphApplicationMaster appMaster = new GiraphApplicationMaster(nodeManager, containerId);

        appMaster.init(new Configuration());

        System.exit(appMaster.run() ? 0 : 1);
    }

    private Configuration conf;
    private YarnRPC rpc;
    private InetSocketAddress nodeManager;
    private ApplicationAttemptId appAttemptID;

    // Handle to communicate with the Resource Manager
    private AMRMProtocol resourceManager;

    public GiraphApplicationMaster(InetSocketAddress nodeManager, ContainerId containerId) {
        this.nodeManager = nodeManager;
        appAttemptID = containerId.getApplicationAttemptId();

        if (LOG.isInfoEnabled()) {
            LOG.info("Application master for app"
                    + ", appId=" + appAttemptID.getApplicationId().getId()
                    + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
                    + ", attemptId=" + appAttemptID.getAttemptId());
        }
    }

    public void init(Configuration conf) {
        this.conf = conf;
        this.conf.addResource("job.xml");
        rpc = YarnRPC.create(conf);

        resourceManager = connectToRM();
    }


    public boolean run() throws YarnRemoteException {
        // register the AM with the RM
        RegisterApplicationMasterResponse response = registerToRM();

        // Dump out information about cluster capability as seen by the resource manager
        int minMem = response.getMinimumResourceCapability().getMemory();
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Min mem capabililty of resources in this cluster " + minMem);
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        this.containerMemory = Util.getMemorySize(conf.getInt("giraph.job.resource.mb", 1024), minMem, maxMem);

        int minWorkers = conf.getInt("giraph.minWorkers", 1);
//        int maxWorkers = conf.getInt("giraph.maxWorkers", 1);

        // launch containers
        boolean appDone = false;
        int totalContainers = minWorkers;
        List<Thread> launchThreads = new ArrayList<Thread>();

        AtomicInteger numCompltedContainers = new AtomicInteger();
        while (!appDone && numCompltedContainers.get() < minWorkers && numFailedContainers.get() == 0) {
            //TODO need a better way to figure out if all the workers are done

            // log current state
            LOG.info("Current application state: "
                    + ", appDone=" + appDone
                    + ", total=" + totalContainers
                    + ", currentAllocated=" + numAllocatedContainers);

            // Sleep before each loop when asking RM for containers
            // to avoid flooding RM with spurious requests when it
            // need not have any available containers
            // Sleeping for 1000 ms.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.info("Sleep interrupted " + e.getMessage());
            }

            // request containers from the RM
            List<ResourceRequest> resourceRequests = new ArrayList<ResourceRequest>();
            ResourceRequest containerAsk = setupContainerAskForRM(totalContainers);
            resourceRequests.add(containerAsk);

            // Send the request to RM
            LOG.info("Asking RM for containers"
                    + ", askCount=" + totalContainers);
            AMResponse amResp = sendContainerAskToRM(resourceRequests);

            // Retrieve list of allocated containers from the response
            List<Container> allocatedContainers = amResp.getAllocatedContainers();
            LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());

            FileSystem fs;
            try {

                fs = FileSystem.get(this.conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerState" + allocatedContainer.getState()
                        + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());
                //						+ ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, fs);
                Thread launchThread = new Thread(runnableLaunchContainer);

                // launch and start the container on a separate thread to keep the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();

            }

            // Check what the current available resources in the cluster are
            // TODO should we do anything if the available resources are not enough?
            Resource availableResources = amResp.getAvailableResources();
            LOG.info("Current available resources in the cluster " + availableResources);

            // Check the completed containers
            List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
            LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info("Got container status for containerID= " + containerStatus.getContainerId()
                        + ", state=" + containerStatus.getState()
                        + ", exitStatus=" + containerStatus.getExitStatus()
                        + ", diagnostics=" + containerStatus.getDiagnostics());

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container failed
                    numCompletedContainers.incrementAndGet();
                    numFailedContainers.incrementAndGet();
                    break; // if one fails just end
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully."
                            + ", containerId=" + containerStatus.getContainerId());
                }

            }
            if (numCompletedContainers.get() == totalContainers || numFailedContainers.get() > 0) {
                appDone = true;
            }

            LOG.info("Current application state: "
                    + ", appDone=" + appDone
                    + ", total=" + totalContainers
                    + ", completed=" + numCompletedContainers
                    + ", failed=" + numFailedContainers
                    + ", currentAllocated=" + numAllocatedContainers);

        }


        // When the application completes, it should send a finish application signal
        // to the RM
        FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
        finishReq.setAppAttemptId(appAttemptID);
        boolean isSuccess = true;
        if (numFailedContainers.get() == 0) { //TODO add real logic here to find out how many containers failed
            finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        } else {
            finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
            String diagnostics = "Diagnostics."
              + ", total=" + totalContainers
              + ", completed=" + numCompletedContainers.get()
              + ", allocated=" + numAllocatedContainers.get()
              + ", failed=" + numFailedContainers.get();
            finishReq.setDiagnostics(diagnostics);
            isSuccess = false;
        }

        LOG.info("Application completed with status " + finishReq.getFinalApplicationStatus() + ". Signalling finish to RM");
        resourceManager.finishApplicationMaster(finishReq);
        return isSuccess;
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
//        appMasterRequest.setTrackingUrl("");

        return resourceManager.registerApplicationMaster(appMasterRequest);
    }


    /**
     * Ask RM to allocate given no. of containers to this Application Master
     *
     * @param requestedContainers Containers to ask for from RM
     * @return Response from RM to AM with allocated containers
     * @throws YarnRemoteException
     */
    private AMResponse sendContainerAskToRM
    (List<ResourceRequest> requestedContainers)
            throws YarnRemoteException {
        AllocateRequest req = Records.newRecord(AllocateRequest.class);
        req.setResponseId(rmRequestID.incrementAndGet());
        req.setApplicationAttemptId(appAttemptID);
        req.addAllAsks(requestedContainers);
//        req.addAllReleases(releasedContainers);  //TODO need to handle thi
        req.setProgress(0.0F);

        LOG.info("Sending request to RM for containers"
                + ", requestedSet=" + requestedContainers.size()
//                + ", releasedSet=" + releasedContainers.size()
                + ", progress=" + req.getProgress());

        for (ResourceRequest rsrcReq : requestedContainers) {
            LOG.info("Requested container ask: " + rsrcReq.toString());
        }
//        for (ContainerId id : releasedContainers) {
//            LOG.info("Released container, id=" + id.getId());
//        }

        AllocateResponse resp = resourceManager.allocate(req);
        return resp.getAMResponse();
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @param numContainers Containers to ask for from RM
     * @return the setup ResourceRequest to be sent to RM
     */
    private ResourceRequest setupContainerAskForRM
    (
            int numContainers) {
        ResourceRequest request = Records.newRecord(ResourceRequest.class);

        // setup requirements for hosts
        // whether a particular rack/host is needed
        // Refer to apis under org.apache.hadoop.net for more
        // details on how to get figure out rack/host mapping.
        // using * as any host will do for the distributed shell app
        request.setHostName("*");

        // set no. of containers needed
        request.setNumContainers(numContainers);

        // set the priority for the request
        Priority pri = Records.newRecord(Priority.class);
        // TODO - what is the range for priority? how to decide?
        pri.setPriority(0);
        request.setPriority(pri);

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        request.setCapability(capability);

        return request;
    }


    /**
     * Connect to the Resource Manager
     *
     * @return Handle to communicate with the RM
     */
    private AMRMProtocol connectToRM
    () {
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
                YarnConfiguration.RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
        LOG.info("Connecting to ResourceManager at " + rmAddress);
        return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
    }

    /**
     * Thread to connect to the {@link org.apache.hadoop.yarn.api.ContainerManager} and
     * launch the container that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        Container container;
        // Handle to communicate with ContainerManager
        ContainerManager cm;
        FileSystem fileSystem;

        /**
         * @param lcontainer Allocated container
         */
        public LaunchContainerRunnable(Container lcontainer, FileSystem fileSystem) {
            this.container = lcontainer;
            this.fileSystem = fileSystem;
        }

        /**
         * Helper function to connect to CM
         */
        private void connectToCM() {
            String cmIpPortStr = container.getNodeId().getHost() + ":"
                    + container.getNodeId().getPort();
            InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
            LOG.info("Connecting to ResourceManager at " + cmIpPortStr);
            this.cm = ((ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, conf));
        }


        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        public void run() {
            // Connect to ContainerManager
            LOG.info("Connecting to container manager for containerid=" + container.getId());
            connectToCM();

            LOG.info("Setting up container launch container for containerid=" + container.getId());
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

            ctx.setContainerId(container.getId());
            ctx.setResource(container.getResource());

            try {
                ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
            } catch (IOException e) {
                LOG.info("Getting current user info failed when trying to launch the container"
                        + e.getMessage());
            }

            // set local resources to the staging dir
            try {
                Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
                Path jobStagingDir = Util.getStagingDir(conf, container.getId().getApplicationAttemptId().getApplicationId());
                Util.addPathContentAsResource(fileSystem, jobStagingDir, localResources);
                ctx.setLocalResources(localResources);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // setup container command + envo
            Util.setupContainerCommand(ctx, WorkerContainer.class);


            StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
            startReq.setContainerLaunchContext(ctx);
            try {
                cm.startContainer(startReq);
            } catch (YarnRemoteException e) {
                LOG.info("Start container failed for :"
                        + ", containerId=" + container.getId());
                e.printStackTrace();
                // TODO do we need to release this container?
            }

            // Get container status
            GetContainerStatusRequest statusReq = Records.newRecord(GetContainerStatusRequest.class);
            statusReq.setContainerId(container.getId());
            GetContainerStatusResponse statusResp;
            try {
                statusResp = cm.getContainerStatus(statusReq);
                LOG.info("Container Status"
                        + ", id=" + container.getId()
                        + ", status=" +statusResp.getStatus());
            } catch (YarnRemoteException e) {
                e.printStackTrace();
            }
        }
    }
}
