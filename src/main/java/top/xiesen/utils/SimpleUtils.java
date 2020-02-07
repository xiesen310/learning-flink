package top.xiesen.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class SimpleUtils {

    public static List<String> getJarName(String className) throws Exception {
        //boot类加载器加载的jar路径
        String bootPath = System.getProperty("sun.boot.class.path");
        //ext类加载器加载的jar路径
        String extPath = System.getProperty("java.ext.dirs");
        //所有的jars
        String allPath = System.getProperty("java.class.path");
        //windows平台是; linux平台是:  ，最好使用File.pathSeparatorChar做跨平台
        String[] jarnames = allPath.split(";");

        List<String> jars = new ArrayList<>();

        for (int i = 0; i < jarnames.length; i++) {
            File file = new File(jarnames[i]);
            if (file.isFile() && file.exists()) {
                JarFile jar = new JarFile(file);
                Enumeration<JarEntry> enums = jar.entries();
                while (enums.hasMoreElements()) {
                    JarEntry entry = enums.nextElement();
                    String qulifierName = entry.getName().replace("/", ".");
                    if (qulifierName.equals(className)) {
                        jars.add("class name =  "+qulifierName + " , jar filename = " + file.getName());
                    }
                }
            }
        }

        return jars;
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(getJarName("java.util.concurrent.ThreadPoolExecutor.class"));
//        System.out.println(getJarName("org.apache.flink.api.java.ExecutionEnvironment.class"));
        System.out.println("hello world");
    }
}
