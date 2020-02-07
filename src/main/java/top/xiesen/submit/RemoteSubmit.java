package top.xiesen.submit;

import org.apache.flink.api.common.JobSubmissionResult;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Richy
 * @date Create in 13:58 2019/6/10
 */
public class RemoteSubmit {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSubmit.class);

    public static void main(String[] args) throws Exception {

        // 集群信息
        Configuration configuration = new Configuration();

        configuration.setString(JobManagerOptions.ADDRESS, "192.168.1.222");
        configuration.setInteger(JobManagerOptions.PORT, 6123);
        configuration.setInteger(RestOptions.PORT, 8081);

        // rest客户端
        RestClusterClient restClient = new RestClusterClient(configuration, "RemoteExecutor");
        restClient.setPrintStatusDuringExecution(true);
        restClient.setDetached(true);

        // 定义参数
        // jar 包
//        File file = new File("D:\\temp\\learning-flink-1.0.jar");
        File file = new File(args[0]);
        // 参数
        List<String> programArgs = new ArrayList<String>();

        PackagedProgram program = new PackagedProgram(file, "top.xiesen.batch.WordCount",
                programArgs.toArray(new String[programArgs.size()]));
        ClassLoader classLoader = null;
        try {
            classLoader = program.getUserCodeClassLoader();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
        }
        // 优化控制器
        Optimizer optimizer = new Optimizer(
                //
                new DataStatistics(),
                // 消耗估算
                new DefaultCostEstimator(),
                // 配置, CoreOptions
                new Configuration());
        FlinkPlan plan = ClusterClient.getOptimizedPlan(optimizer, program, 1);

        // Savepoint restore settings
        SavepointRestoreSettings savepointSettings = SavepointRestoreSettings.none();
        /*String savepointPath = "/zork/";

        if (StringUtils.isNotEmpty(savepointPath)) {
            Boolean allowNonRestoredOpt = true;
            boolean allowNonRestoredState = allowNonRestoredOpt != null && allowNonRestoredOpt.booleanValue();
            savepointSettings = SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);
        }*/

        // set up the execution environment
        List<URL> jarFiles = FileUtil.createPath(file);
        JobSubmissionResult submissionResult
                = restClient.run(plan, jarFiles, Collections.emptyList(), classLoader, savepointSettings);


        System.out.println(" submit jar request sucess,jobId:{}" + submissionResult.getJobID());

    }


}
