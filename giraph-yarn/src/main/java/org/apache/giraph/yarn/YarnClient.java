package org.apache.giraph.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Client that will launch a giraph AM
 */
public class YarnClient {
    private static final Log LOG = LogFactory.getLog(YarnClient.class);

    private Configuration conf;

    private YarnRPC rpc;
    private ClientRMProtocol applicationsManager;
    private FileSystem fs;

    private int amMemory;
    private Path stagingDir;
    private ApplicationId appId;

    // how much memory yarn allows
    private int minMem, maxMem;

    private String jobJar = Util.getJarPath(YarnClient.class).getAbsolutePath();
    private long clientTimeout = 600000L;
    private long clientStartTime;

    public void init(Configuration conf) throws IOException {
        this.conf = new Configuration(conf);
        rpc = YarnRPC.create(conf);
        fs = FileSystem.get(conf);
    }

    public void run() throws IOException {
        LOG.info("Starting Client");
        clientStartTime = System.currentTimeMillis();

        // connect to the ASM
        connectToASM();
        assert (applicationsManager != null);

        // log cluster status
        logClusterStatus();

        // get new application
        GetNewApplicationResponse newApp = getApplication();
        appId = newApp.getApplicationId();

        minMem = newApp.getMinimumResourceCapability().getMemory();
        maxMem = newApp.getMaximumResourceCapability().getMemory();

        // create submission context
        LOG.info("Setting up application submission context for ASM");
        ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

        // setup application context
        initApplicationContext(appContext);

        // Create the request to send to the applications manager
        SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
        appRequest.setApplicationSubmissionContext(appContext);

        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
        // Ignore the response as either a valid response object is returned on success
        // or an exception thrown to denote some form of a failure
        LOG.info("Submitting application to ASM");
        applicationsManager.submitApplication(appRequest);
    }

    public boolean waitForCompletion(boolean wait) throws IOException {
        run();
        if(wait) {
            return monitorApplication(appId);
        } else {
            // if we got to this point, then we have submitted the job
            return true;
        }
    }

    private void initApplicationContext(ApplicationSubmissionContext appContext) throws IOException {
        // determin staging dir
        this.stagingDir = Util.getStagingDir(this.conf, appId);

        // set the application id
        appContext.setApplicationId(appId);
        // set the application name
        appContext.setApplicationName(conf.get("giraph.yarn.job.name", "Giraph Application"));

        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(conf.getInt("giraph.yarn.job.priority", 0));
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(conf.get("giraph.yarn.job.queue", YarnConfiguration.DEFAULT_QUEUE_NAME));

        // set the user
        appContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName());

        // get container
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // setup container
        initContainerContext(amContainer);

        // add container to app context
        appContext.setAMContainerSpec(amContainer);
    }

    private void initContainerContext(ContainerLaunchContext amContainer) throws IOException {
        // setup user for container
        amContainer.setUser(UserGroupInformation.getCurrentUser().getShortUserName());

        // determin AM memory needed
        determinAMMemorySize();

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        amContainer.setResource(capability);

        setupLocalResources(amContainer);

        Util.setupContainerCommand(amContainer, GiraphApplicationMaster.class);
    }

    private void addResourceToStagingDir(Path src, String destName, Map<String, LocalResource> localResources) throws IOException {
        Path dst = new Path(this.stagingDir, destName);
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);
        LOG.info("Added file ("+destName+") to Staging dir ("+stagingDir+"): " + dst);

        //hdfs://localhost:56942/tmp/hadoop/giraph/staging/dcapwell/.staging/application_1321078079616_0001/job.xml

        LocalResource localResource = Util.getDefaultLocalResource();
        // Set the resource to be copied over
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        // Set timestamp and length of file so that the framework
        // can do basic sanity checks for the local resource
        // after it has been copied over to ensure it is the same
        // resource the client intended to use with the application
        localResource.setTimestamp(destStatus.getModificationTime());
        localResource.setSize(destStatus.getLen());

        localResources.put(destName, localResource);
    }

    private void setupLocalResources(ContainerLaunchContext amContainer) throws IOException {
        // Setup staging dir
        addToStagingDir(new Path(jobJar), "job.jar");

        File jobxml = File.createTempFile("job", ".xml");
        conf.writeXml(new FileOutputStream(jobxml, false));
        addToStagingDir(new Path(jobxml.getAbsolutePath()), "job.xml");

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        // get all contents of staging and add here
        Util.addPathContentAsResource(fs, stagingDir, localResources);

//        LOG.info("Copy App Master jar from local filesystem and add to local environment");
//        // Copy the application master jar to the filesystem
//        // Create a local resource to point to the destination jar path
//        addResourceToStagingDir(new Path(jobJar), "job.jar", localResources);

        // set the job.xml from conf
//        File jobxml = File.createTempFile("job", ".xml");
//        conf.writeXml(new FileOutputStream(jobxml, false));
//        addResourceToStagingDir(new Path(jobxml.getAbsolutePath()), "job.xml", localResources);

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);
    }

    private void addToStagingDir(Path src, String destName) throws IOException {
        Path dst = new Path(this.stagingDir, destName);
        fs.copyFromLocalFile(false, true, src, dst);
    }


    private void connectToASM() {
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
                YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS));
        LOG.info("Connecting to ResourceManager at " + rmAddress);
        applicationsManager = ((ClientRMProtocol) rpc.getProxy(
                ClientRMProtocol.class, rmAddress, conf));
    }

    private void determinAMMemorySize() {
        amMemory = conf.getInt("giraph.yarn.am.mb", 1024);

        amMemory = Util.getMemorySize(amMemory, minMem, maxMem);
    }

    /**
     * Get a new application from the ASM
     *
     * @return New Application
     * @throws YarnRemoteException
     */
    private GetNewApplicationResponse getApplication() throws YarnRemoteException {
        GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
        GetNewApplicationResponse response = applicationsManager.getNewApplication(request);
        LOG.info("Got new application id=" + response.getApplicationId());
        return response;
    }

    private void logClusterStatus() throws YarnRemoteException {
        if (LOG.isDebugEnabled()) {
            // Use ClientRMProtocol handle to general cluster information
            GetClusterMetricsRequest clusterMetricsReq = Records.newRecord(GetClusterMetricsRequest.class);
            GetClusterMetricsResponse clusterMetricsResp = applicationsManager.getClusterMetrics(clusterMetricsReq);
            LOG.debug("Got Cluster metric info from ASM"
                    + ", numNodeManagers=" + clusterMetricsResp.getClusterMetrics().getNumNodeManagers());

            GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
            GetClusterNodesResponse clusterNodesResp = applicationsManager.getClusterNodes(clusterNodesReq);
            LOG.debug("Got Cluster node info from ASM");
            for (NodeReport node : clusterNodesResp.getNodeReports()) {
                LOG.debug("Got node report from ASM for"
                        + ", nodeId=" + node.getNodeId()
                        + ", nodeAddress" + node.getHttpAddress()
                        + ", nodeRackName" + node.getRackName()
                        + ", nodeNumContainers" + node.getNumContainers()
                        + ", nodeHealthStatus" + node.getNodeHealthStatus());
            }

            GetQueueInfoRequest queueInfoReq = Records.newRecord(GetQueueInfoRequest.class);
            GetQueueInfoResponse queueInfoResp = applicationsManager.getQueueInfo(queueInfoReq);
            QueueInfo queueInfo = queueInfoResp.getQueueInfo();
            LOG.debug("Queue info"
                    + ", queueName=" + queueInfo.getQueueName()
                    + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                    + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                    + ", queueApplicationCount=" + queueInfo.getApplications().size()
                    + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

            GetQueueUserAclsInfoRequest queueUserAclsReq = Records.newRecord(GetQueueUserAclsInfoRequest.class);
            GetQueueUserAclsInfoResponse queueUserAclsResp = applicationsManager.getQueueUserAcls(queueUserAclsReq);
            List<QueueUserACLInfo> listAclInfo = queueUserAclsResp.getUserAclsInfoList();
            for (QueueUserACLInfo aclInfo : listAclInfo) {
                for (QueueACL userAcl : aclInfo.getUserAcls()) {
                    LOG.debug("User ACL Info for Queue"
                            + ", queueName=" + aclInfo.getQueueName()
                            + ", userAcl=" + userAcl.name());
                }
            }
        }
    }

    /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnRemoteException
   */
  private boolean monitorApplication(ApplicationId appId) throws YarnRemoteException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      GetApplicationReportRequest reportRequest = Records.newRecord(GetApplicationReportRequest.class);
      reportRequest.setApplicationId(appId);
      GetApplicationReportResponse reportResponse = applicationsManager.getApplicationReport(reportRequest);
      ApplicationReport report = reportResponse.getApplicationReport();

      LOG.info("Got application report from ASM for"
          + ", appId=" + appId.getId()
          + ", clientToken=" + report.getClientToken()
          + ", appDiagnostics=" + report.getDiagnostics()
          + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        }
        else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      }
      else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }

      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        LOG.info("Reached client specified timeout for application. Killing application");
        killApplication(appId);
        return false;
      }
    }
  }

    /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed.
   * @throws YarnRemoteException
   */
  private void killApplication(ApplicationId appId) throws YarnRemoteException {
    KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
    // the same time.
    // If yes, can we kill a particular attempt only?
    request.setApplicationId(appId);
    // KillApplicationResponse response = applicationsManager.forceKillApplication(request);
    // Response can be ignored as it is non-null on success or
    // throws an exception in case of failures
    applicationsManager.forceKillApplication(request);
  }

}
