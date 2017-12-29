package hadoop.Writable.Serialization;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//LongWritable 是因为所有参数都是long类型
public class MyMapper extends Mapper<LongWritable, Text, Text,KpiWritable>{
	//这个的key是对象，也可以用他们的类
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, KpiWritable>.Context context)
			throws IOException, InterruptedException {
		String[] spilted = key.toString().split("\t");
		String string = spilted[1];//手机号码
		Text k2 = new Text(string);//手机号码作为主键
	    KpiWritable v2 = new KpiWritable(spilted[6], spilted[7],spilted[8], spilted[9]);
	    context.write(k2, v2);
	}
}
