package hadoop.Writable.Serialization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text,KpiWritable, Text, KpiWritable>{

	@Override
	protected void reduce(Text k2, Iterable<KpiWritable> arg1,
			Reducer<Text, KpiWritable, Text, KpiWritable>.Context context) throws IOException, InterruptedException {
		//用于统计的参数
		 long upPackNum = 0L;
         long downPackNum = 0L;
         long upPayLoad = 0L;
         long downPayLoad = 0L;
		for (KpiWritable kpiWritable : arg1) {
			upPackNum+=kpiWritable.getUpPackNum();
			downPackNum+=kpiWritable.getDownPackNum();
			upPayLoad+=kpiWritable.getUpPayLoad();
			downPayLoad+=kpiWritable.getDownPayLoad();
		}
		//k2这里的k2是mapper拆分的key，也就是手机号码
		KpiWritable v3 = new KpiWritable(upPackNum, downPackNum, upPayLoad, downPayLoad);
		context.write(k2, v3);
	}
}
