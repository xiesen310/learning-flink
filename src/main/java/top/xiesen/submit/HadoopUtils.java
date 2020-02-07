package top.xiesen.submit;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HadoopUtils {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.runtime.util.HadoopUtils.class);

    private static final Text HDFS_DELEGATION_TOKEN_KIND = new Text("HDFS_DELEGATION_TOKEN");

    @SuppressWarnings("deprecation")
    public static Configuration getHadoopConfiguration(org.apache.flink.configuration.Configuration flinkConfiguration) {

        // Instantiate a HdfsConfiguration to load the hdfs-site.xml and hdfs-default.xml
        // from the classpath
        Configuration result = new HdfsConfiguration();
        boolean foundHadoopConfiguration = false;

        // We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
        // the hdfs configuration
        // Try to load HDFS configuration from Hadoop's own configuration files
        // 1. approach: Flink configuration
        final String hdfsDefaultPath =
                flinkConfiguration.getString(ConfigConstants.HDFS_DEFAULT_CONFIG, null);

        if (hdfsDefaultPath != null) {
            result.addResource(new org.apache.hadoop.fs.Path(hdfsDefaultPath));
            LOG.debug("Using hdfs-default configuration-file path form Flink config: {}", hdfsDefaultPath);
            foundHadoopConfiguration = true;
        } else {
            LOG.debug("Cannot find hdfs-default configuration-file path in Flink config.");
        }

        final String hdfsSitePath = flinkConfiguration.getString(ConfigConstants.HDFS_SITE_CONFIG, null);
        if (hdfsSitePath != null) {
            result.addResource(new org.apache.hadoop.fs.Path(hdfsSitePath));
            LOG.debug("Using hdfs-site configuration-file path form Flink config: {}", hdfsSitePath);
            foundHadoopConfiguration = true;
        } else {
            LOG.debug("Cannot find hdfs-site configuration-file path in Flink config.");
        }

        // 2. Approach environment variables
        String[] possibleHadoopConfPaths = new String[4];
        possibleHadoopConfPaths[0] = flinkConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
        possibleHadoopConfPaths[1] = System.getenv("HADOOP_CONF_DIR");

        final String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome != null) {
            possibleHadoopConfPaths[2] = hadoopHome + "/conf";
            possibleHadoopConfPaths[3] = hadoopHome + "/etc/hadoop"; // hadoop 2.2
        }

        for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
            if (possibleHadoopConfPath != null) {
                if (new File(possibleHadoopConfPath).exists()) {
                    if (new File(possibleHadoopConfPath + "/core-site.xml").exists()) {
                        result.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/core-site.xml"));
                        LOG.debug("Adding " + possibleHadoopConfPath + "/core-site.xml to hadoop configuration");
                        foundHadoopConfiguration = true;
                    }
                    if (new File(possibleHadoopConfPath + "/hdfs-site.xml").exists()) {
                        result.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/hdfs-site.xml"));
                        LOG.debug("Adding " + possibleHadoopConfPath + "/hdfs-site.xml to hadoop configuration");
                        foundHadoopConfiguration = true;
                    }
                }
            }
        }

        if (!foundHadoopConfiguration) {
            LOG.debug("Could not find Hadoop configuration via any of the supported methods " +
                    "(Flink configuration, environment variables).");
        }

        return result;
    }
}
