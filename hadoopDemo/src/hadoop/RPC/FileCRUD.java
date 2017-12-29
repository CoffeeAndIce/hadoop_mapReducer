package hadoop.RPC;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
* @ClassName: WordCount
* @Description: 统计单词
* @author linge
* @date 2017年12月28日 下午3:22:21
*
*/
public class FileCRUD {
	private  static final String hdfs = "hdfs://192.168.88.129:9000";
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.name.default", hdfs);
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		System.setProperty("hadoop.home.dir", "E:/lg_file/leaning/hadoop-3.0.0-alpha4");
		
		//获得filesystem对象
		FileSystem fs = FileSystem.get(new URI(hdfs),conf,"root");
//		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
//		while (listFiles.hasNext()) {
//			System.out.println(listFiles.next().toString());
//		}
//		fs.mkdirs(new Path("/linge"));
//		fs.copyFromLocalFile(new Path("E:/lg_workspace/hadoopDemo/root/ha.txt"), new Path(hdfs+"/linge.txt"));
		//文件上传
		upLoadMethod(fs,"/ha.txt");
//		fs.delete(new Path("/linge"),true);
		//文件复制api
//		fs.copyToLocalFile(new Path(hdfs+"/linge.txt"),new Path("E:/lg_workspace/hadoopDemo/root/linge.txt"));
		
		//文件下载
//		downLoadMethod(fs,"/ha.txt");
				
	}
	/**
	* @Title: downLoadMethod
	* @Description: 底层调用下载方法
	* @param fs   filesystem对象
	* @param path   文件相对路径
	* @throws IOException
	* @throws FileNotFoundException    设定文件 
	* @return void    返回类型 
	* @throws
	*/
	private static void downLoadMethod(FileSystem fs, String path) throws IOException, FileNotFoundException {
		//打开输入流
		FSDataInputStream in = fs.open(new Path(hdfs+path));
		//设置输出流
		FileOutputStream out = new FileOutputStream("E:\\lg_workspace\\hadoopDemo\\root"+path);
		//调用IOUtils的copy方法，复制文件
		IOUtils.copy(in, out);
	}
	/**
	* @Title: upLoadMethod
	* @Description: 底层调用上传方法
	* @param fs   filesystem对象
	* @param path   文件相对路径
	* @throws IOException
	* @throws FileNotFoundException    设定文件 
	* @return void    返回类型 
	* @throws
	*/
	@SuppressWarnings("unused")
	private static void upLoadMethod(FileSystem fs, String path) throws IOException, FileNotFoundException {
		//打开输入流
		FileInputStream in = new FileInputStream("E:/lg_workspace/hadoopDemo/root/"+path);
		//设置输出流
		FSDataOutputStream  out = fs.create(new Path(hdfs+path));
		//调用IOUtils的copy方法，复制文件
		IOUtils.copy(in, out);
	}
}
