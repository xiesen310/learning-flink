package top.xiesen.submit;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class YarnSubmit {
    private static YarnConfiguration yarnConfiguration;


    private static YarnClient yarnClient;
    @Rule
    private static TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static void main(String[] args) throws Exception {
        yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("yarn.resourcemanager.address", "zork-poc103:8032");
        yarnConfiguration.set("fs.default.name", "hdfs://zork-poc103:8020");
        yarnConfiguration.set("yarn.resourcemanager.hostname", "zork-poc103");
        yarnConfiguration.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        yarnConfiguration.set("mapreduce.job.ubertask.enable", "true");

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        Configuration configuration = new Configuration();
        configuration.setString("yarn.per-job-cluster.include-user-jar", "FIRST");

//        String path = "D:\\temp\\learning-flink-1.0.jar";
        String path = args[0];
        File flinkJar = new File(path);

        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                path,
                yarnClient,
                false);

        yarnClusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(1024)
                .setTaskManagerMemoryMB(1)
                .setNumberTaskManagers(1024)
                .setSlotsPerTaskManager(1)
                .createClusterSpecification();

        String applicationName = "flink-test";
        String yarnClusterEntrypoint = "";
        JobGraph jobGraph = new JobGraph();
        YarnClientApplication yarnApplication = yarnClient.createApplication();

        try {
//            yarnClusterDescriptor.deploySessionCluster(clusterSpecification);
            ApplicationReport report = yarnClusterDescriptor.startAppMaster(configuration,
                    applicationName,
                    yarnClusterEntrypoint,
                    jobGraph,
                    yarnClient,
                    yarnApplication,
                    clusterSpecification);

        } catch (ClusterDeploymentException e) {
            if (!(e.getCause() instanceof IllegalConfigurationException)) {
                throw e;
            }
        } finally {
            yarnClusterDescriptor.close();
        }

    }
}
