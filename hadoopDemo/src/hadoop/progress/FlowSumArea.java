package hadoop.progress;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.Writable.Deserialization.MyKpiJob;
import hadoop.Writable.Deserialization.MyKpiJob.KpiWritable;
import hadoop.Writable.Deserialization.MyKpiJob.MyMapper;
import hadoop.Writable.Deserialization.MyKpiJob.MyReducer;

/**
 * 对流量原始日志进行流量统计，将不同省份的用户统计结果输出到不同文件
 * 需要自定义改造两个机制：
 * 1、改造分区的逻辑，自定义一个partitioner
 * 2、自定义reduer task的并发任务数
 * 
 */
public class FlowSumArea extends Configured implements Tool{
    public static void main(String[] args) {
    	Long f = System.currentTimeMillis();
        Configuration conf = new Configuration();
//        conf.setBoolean("mapred.compress.map.output", true);   
//        conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        try {
            int res = ToolRunner.run(conf, new FlowSumArea(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Long e = System.currentTimeMillis();
        System.out.println(e-f);
    }
    // 输入文件目录
    public static final String INPUT_PATH = "hdfs://192.168.88.129:9000/ha.txt";
    // 输出文件目录
    public static final String OUTPUT_PATH = "hdfs://192.168.88.129:9000/out";
    public int run(String[] args) throws Exception {
        // 首先删除输出目录已生成的文件
        FileSystem fs = FileSystem.get(new URI(INPUT_PATH), getConf());
        Path outPath = new Path(OUTPUT_PATH);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        // 定义一个作业
        Job job = new Job(getConf(), "MyKpiJob");
        // 分区需要设置为打包运行
        job.setJarByClass(MyKpiJob.class);
        // 设置输入目录
        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        // 设置自定义Mapper类
        job.setMapperClass(MyMapper.class);
        // 指定<k2,v2>的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(KpiWritable.class);
        // 设置Partitioner
        job.setPartitionerClass(KpiPartitioner.class);
        //设置节点数
        job.setNumReduceTasks(2);
        // 设置Combiner
        job.setCombinerClass(MyReducer.class);
        // 设置自定义Reducer类
        job.setReducerClass(MyReducer.class);
        // 指定<k3,v3>的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(KpiWritable.class);
        // 设置输出目录
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        // 提交作业
        FileOutputFormat.setCompressOutput(job, true);  //job使用压缩  
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); //设置压缩格式  
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}