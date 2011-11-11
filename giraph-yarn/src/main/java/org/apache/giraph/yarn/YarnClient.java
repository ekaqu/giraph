package org.apache.giraph.yarn;

import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Apps;
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
        this.stagingDir = getStagingDir(this.conf, appId);

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

        setupContainerCommand(amContainer);
    }

    private void setupContainerCommand(ContainerLaunchContext amContainer) {
        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");

        // Set class name
        vargs.add(conf.getClass("giraph.yarn.am.impl", GiraphApplicationMaster.class).getName());

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        amContainer.setCommands(commands);

        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        // Add job.jar location to classpath
        Apps.addToEnvironment(env,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + Path.SEPARATOR +"job.jar");
        Apps.addToEnvironment(env,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + Path.SEPARATOR + "*");

        // Add standard Hadoop classes
          for (String c : ApplicationConstants.APPLICATION_CLASSPATH) {
            Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), c);
          }

        // for mini yarn cluster, the above application_classpath will not work, so need autogenerated
        // add the runtime classpath needed for tests to work
        String testRuntimeClassPath = getTestRuntimeClasspath();
        if(!Strings.isNullOrEmpty(testRuntimeClassPath)) {
            Apps.addToEnvironment(env,
                ApplicationConstants.Environment.CLASSPATH.name(),
                testRuntimeClassPath);
        }

        amContainer.setEnvironment(env);
    }

    private static String getTestRuntimeClasspath() {

    InputStream classpathFileStream = null;
    BufferedReader reader = null;
    String envClassPath = "";

    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    try {

      // Create classpath from generated classpath
      // Check maven pom.xml for generated classpath info
      // Works if compile time env is same as runtime. Mainly tests.
      ClassLoader thisClassLoader =
          Thread.currentThread().getContextClassLoader();
      String generatedClasspathFile = "giraph-yarn-app-generated-classpath";
      classpathFileStream =
          thisClassLoader.getResourceAsStream(generatedClasspathFile);
      if (classpathFileStream == null) {
        LOG.info("Could not classpath resource from class loader");
        return envClassPath;
      }
      LOG.info("Readable bytes from stream=" + classpathFileStream.available());
      reader = new BufferedReader(new InputStreamReader(classpathFileStream));
      String cp = reader.readLine();
      if (cp != null) {
        envClassPath += cp.trim() + ":";
      }
      // Put the file itself on classpath for tasks.
      envClassPath += thisClassLoader.getResource(generatedClasspathFile).getFile();
    } catch (IOException e) {
      LOG.info("Could not find the necessary resource to generate class path for tests. Error=" + e.getMessage());
    }

    try {
      if (classpathFileStream != null) {
        classpathFileStream.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      LOG.info("Failed to close class path file stream or reader. Error=" + e.getMessage());
    }
    return envClassPath;
  }

    private static LocalResource getDefaultLocalResource() {
        LocalResource localResource = Records.newRecord(LocalResource.class);

        // Set the type of resource - file or archive
        // archives are untarred at destination
        // we don't need the jar file to be untarred for now
        localResource.setType(LocalResourceType.FILE);
        // Set visibility of the resource
        // Setting to most private option
        localResource.setVisibility(LocalResourceVisibility.APPLICATION);

        return localResource;
    }

    private void addResourceToStagingDir(Path src, String destName, Map<String, LocalResource> localResources) throws IOException {
        Path dst = new Path(this.stagingDir, destName);
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);

        LocalResource localResource = getDefaultLocalResource();
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
        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        addResourceToStagingDir(new Path(jobJar), "job.jar", localResources);

        // set the job.xml from conf
        File jobxml = File.createTempFile("job", ".xml");
        conf.writeXml(new FileOutputStream(jobxml, false));
        addResourceToStagingDir(new Path(jobxml.getAbsolutePath()), "job.xml", localResources);

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);
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

        // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be
        // a multiple of the min value and cannot exceed the max.
        // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
        if (amMemory < minMem) {
            LOG.info("AM memory specified below min threshold of cluster. Using min value."
                    + ", specified=" + amMemory
                    + ", min=" + minMem);
            amMemory = minMem;
        } else if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem;
        }
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

    public static Path getStagingDir(Configuration conf, ApplicationId appId) throws IOException {
        Path stagingRootDir = new Path(conf.get("giraph.yarn.job.staging.root.dir", "/tmp/hadoop/giraph/staging"));
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String user;
        if(ugi == null) {
            user = "dummy-yarn-user";
        } else {
            user = ugi.getShortUserName();
        }
        stagingRootDir = new Path(stagingRootDir, user + Path.SEPARATOR_CHAR + ".staging");

        FileSystem fs = FileSystem.get(conf);
        return fs.makeQualified(new Path(stagingRootDir, appId.toString()));
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
