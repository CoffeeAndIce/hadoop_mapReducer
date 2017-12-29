package hadoop.Writable.Deserialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyKpiJob extends Configured implements Tool {

    /*
     * 自定义数据类型KpiWritable
     */
    public static class KpiWritable implements WritableComparable<KpiWritable>{

        long upPackNum; // 上行数据包数，单位：个
        long downPackNum; // 下行数据包数，单位：个
        long upPayLoad; // 上行总流量，单位：byte
        long downPayLoad; // 下行总流量，单位：byte

        public KpiWritable() {
        }

        public KpiWritable(String upPack, String downPack, String upPay,
                String downPay) {
            upPackNum = Long.parseLong(upPack);
            downPackNum = Long.parseLong(downPack);
            upPayLoad = Long.parseLong(upPay);
            downPayLoad = Long.parseLong(downPay);
        }

        @Override
        public String toString() {
            String result = upPackNum + "\t" + downPackNum + "\t" + upPayLoad
                    + "\t" + downPayLoad;
            return result;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(upPackNum);
            out.writeLong(downPackNum);
            out.writeLong(upPayLoad);
            out.writeLong(downPayLoad);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            upPackNum = in.readLong();
            downPackNum = in.readLong();
            upPayLoad = in.readLong();
            downPayLoad = in.readLong();
        }

		@Override
		public int compareTo(KpiWritable o) {
			// TODO Auto-generated method stub
			return (int) (o.downPackNum-this.downPackNum);
		}

    }

    /*
     * 自定义Mapper类，重写了map方法
     */
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, KpiWritable> {
        protected void map(
                LongWritable k1,
                Text v1,
                org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, KpiWritable>.Context context)
                throws IOException, InterruptedException {
            String[] spilted = v1.toString().split("\\s+");
            if(spilted.length>1){
            String msisdn = spilted[1]; // 获取手机号码
            Text k2 = new Text(msisdn); // 转换为Hadoop数据类型并作为k2
            KpiWritable v2 = new KpiWritable(spilted[6], spilted[7],
                    spilted[8], spilted[9]);
            context.write(k2, v2);
            }
        };
    }

    /*
     * 自定义Reducer类，重写了reduce方法
     */
    public static class MyReducer extends
            Reducer<Text, KpiWritable, Text, KpiWritable> {
        protected void reduce(
                Text k2,
                java.lang.Iterable<KpiWritable> v2s,
                org.apache.hadoop.mapreduce.Reducer<Text, KpiWritable, Text, KpiWritable>.Context context)
                throws IOException, InterruptedException {
            long upPackNum = 0L;
            long downPackNum = 0L;
            long upPayLoad = 0L;
            long downPayLoad = 0L;
            for (KpiWritable kpiWritable : v2s) {
                upPackNum += kpiWritable.upPackNum;
                downPackNum += kpiWritable.downPackNum;
                upPayLoad += kpiWritable.upPayLoad;
                downPayLoad += kpiWritable.downPayLoad;
            }

            KpiWritable v3 = new KpiWritable(upPackNum + "", downPackNum + "",
                    upPayLoad + "", downPayLoad + "");
            System.out.println(k2+"\nupPackNum:"+upPackNum+"/ndownPackNum："+downPackNum+"/n:upPayLoad："+upPayLoad+"/ndownPayLoad："+downPayLoad);
            context.write(k2, v3);
        };
    }

    // 输入文件目录
    public static final String INPUT_PATH = "hdfs://192.168.88.129:9000/ha.txt";
    // 输出文件目录
    public static final String OUTPUT_PATH = "hdfs://192.168.88.129:9000/out";

    @Override
    public int run(String[] args) throws Exception {
//        // 首先删除输出目录已生成的文件
//        FileSystem fs = FileSystem.get(new URI(INPUT_PATH), getConf());
//        Path outPath = new Path(OUTPUT_PATH);
//        if (fs.exists(outPath)) {
//            fs.delete(outPath, true);
//        }
//        // 定义一个作业
//        Job job = Job.getInstance(getConf(),"lingeJob");
//        // 设置输入目录
//        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
//        // 设置自定义Mapper类
//        job.setMapperClass(MyMapper.class);
//        // 指定<k2,v2>的类型
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(KpiWritable.class);
//        // 设置自定义Reducer类
//        job.setReducerClass(MyReducer.class);
//        // 指定<k3,v3>的类型
//        job.setOutputKeyClass(Text.class);
//        job.setOutputKeyClass(KpiWritable.class);
//        // 设置输出目录
//        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
//        // 提交作业
//        Boolean res = job.waitForCompletion(true);
//        if(res){
//            System.out.println("Process success!");
//            System.exit(0);
//        }
//        else{
//            System.out.println("Process failed!");
//            System.exit(1);
//        }
        return 0;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            int res = ToolRunner.run(conf, new MyKpiJob(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}