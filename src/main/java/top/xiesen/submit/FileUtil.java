package top.xiesen.submit;

import org.apache.flink.client.program.JobWithJars;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Richy
 * @date Create in 18:32 2019/8/12
 */
public class FileUtil {

    public static List<URL> createPath(File file) {
        List<URL> jarFiles = new ArrayList<>(1);
        if (file == null) {
            return jarFiles;
        }
        try {
            URL jarFileUrl = file.getAbsoluteFile().toURI().toURL();
            jarFiles.add(jarFileUrl);
            JobWithJars.checkJarFile(jarFileUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("JAR file is invalid '" + file.getAbsolutePath() + "'", e);
        } catch (IOException e) {
            throw new RuntimeException("Problem with jar file " + file.getAbsolutePath(), e);
        }
        return jarFiles;
    }
}
