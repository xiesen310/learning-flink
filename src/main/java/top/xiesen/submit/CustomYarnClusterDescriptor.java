package top.xiesen.submit;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class CustomYarnClusterDescriptor extends AbstractYarnClusterDescriptor {

    public CustomYarnClusterDescriptor(Configuration flinkConfiguration, YarnConfiguration yarnConfiguration, String configurationDirectory, YarnClient yarnClient, boolean sharedYarnClient) {
        super(flinkConfiguration, yarnConfiguration, configurationDirectory, yarnClient, sharedYarnClient);
    }

    @Override
    protected String getYarnSessionClusterEntrypoint() {
        return null;
    }

    @Override
    protected String getYarnJobClusterEntrypoint() {
        return null;
    }

    @Override
    protected ClusterClient<ApplicationId> createYarnClusterClient(AbstractYarnClusterDescriptor descriptor, int numberTaskManagers, int slotsPerTaskManager, ApplicationReport report, Configuration flinkConfiguration, boolean perJobCluster) throws Exception {
        return null;
    }

    @Override
    public ClusterClient<ApplicationId> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) throws ClusterDeploymentException {
        return null;
    }
}
