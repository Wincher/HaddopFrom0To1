package Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class OX0001HelloHDFS {
	/**
	 * 使用这个demo之前请在namenode的hdfs-site.xml中配置
	 *  dfs.permission.enabled false 不然会发生权限验证异常AccessControlException: Permission denied,
	 *  hdfs默认访问权限为linux的用户权限
	 *  默认值为true，如果修改
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		//最简单的访问方法
		//注意url默认是不识别hdfs协议的，这里我们需要给URL添加hdfs协议
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = new URL("hdfs://192.168.0.207:9000/hello-hdfs.txt");
		//url开启流，拿到输入流
		InputStream inputStream = url.openStream();
		//注意这个IOUtils在 org.apache.hadoop.io包下，
		//提供的方法为将输入流，输出到输出流，
		//并且使用的缓冲buf[]大小为4096, true的含义是我们读完输入流的数据后是否自动关闭
		IOUtils.copyBytes(inputStream, System.out, 4096, true);
		
		//Hadoop为我们提供的访问方法，包含很多操作我们使用这个方法
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.0.207:9000");
		FileSystem fileSystem = FileSystem.get(conf);
		
		boolean success = fileSystem.mkdirs(new Path("/wincher"));
		System.out.println("在HDFS下创建文件夹" + (success?"成功":"失败"));
		
		success = fileSystem.exists(new Path("/hello-hdfs.txt"));
		System.out.println("在HDFS下目录是否存在" + (success?"成功":"失败"));
		//参数true 代表真删除文件，如果为false HDFS会将目录或文件放入类似回收站的地方
		success = fileSystem.delete(new Path("/wincher"), true);
		System.out.println("在HDFS下文件是否存在" + (success?"成功":"失败"));
	}
}
