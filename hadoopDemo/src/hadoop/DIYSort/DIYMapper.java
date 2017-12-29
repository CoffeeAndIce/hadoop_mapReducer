package hadoop.DIYSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hadoop.Writable.Serialization.KpiWritable;

public class DIYMapper extends Mapper<LongWritable, Text, KpiWritable, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, KpiWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] spilted = key.toString().split("\\s+");//分割数据
	       System.out.println(spilted.length);
           if(spilted.length>1){
           String msisdn = spilted[1]; // 获取手机号码
           Text k2 = new Text(msisdn); // 转换为Hadoop数据类型并作为k2
           KpiWritable v2 = new KpiWritable(spilted[6], spilted[7],
                   spilted[8], spilted[9]);
           context.write( v2,NullWritable.get());
           }
	}
	
}