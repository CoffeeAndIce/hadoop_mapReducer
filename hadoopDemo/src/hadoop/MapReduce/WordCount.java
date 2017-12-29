package hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
//	private  static final String hdfs = "hdfs://192.168.88.129:9000";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		conf.set("fs.name.default", hdfs);
//		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		System.setProperty("hadoop.home.dir", "e:/lg_file/leaning/hadoop-3.0.0-alpha4");
		Job job = Job.getInstance();  

    //注意：加载main方法所在的类
		job.setJarByClass(WordCount.class);
		//设置Mapper与Reducer的类
		job.setMapperClass(WcMapper.class);
		job.setReducerClass(WcReducer.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.88.129:9000/ha.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.88.129:9000/output"));
//设置输入和输出的相关属性
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.waitForCompletion(true);
	}

}