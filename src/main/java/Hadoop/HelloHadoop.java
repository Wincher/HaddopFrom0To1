import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;

public class HelloHadoop {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.235:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        System.err.println(fileSystem);
        try {
            boolean b = fileSystem.exists(new Path("/hello"));
            System.out.println(b);
        } catch (Exception e) {
            e.printStackTrace();
        }

        boolean success = fileSystem.mkdirs(new Path("/test"));
        System.out.println(success);

        success = fileSystem.delete(new Path("/test"), true);
        System.out.println(success);

        FSDataOutputStream out = fileSystem.create(new Path("/test.data"), true);
        FileInputStream fis = new FileInputStream("D:\\temp");
        IOUtils.copyBytes(fis, out, 4096, true);

        FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
        //System.out.println(statuses.length);
        for(FileStatus status : statuses) {
            System.out.println(status.getPath());
            System.out.println(status.getPermission());
            System.out.println(status.getReplication());
        }
    }
}
