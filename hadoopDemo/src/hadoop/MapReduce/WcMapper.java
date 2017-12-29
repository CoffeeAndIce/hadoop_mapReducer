package hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WcMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//接收数据
		String line = value.toString();
		//切分数据
		String [] words = line.split(" ");
		//循环
		for(String w : words){
			//出现一次，记个1，输出
			context.write(new Text(w), new LongWritable(1));
		}
	}
}