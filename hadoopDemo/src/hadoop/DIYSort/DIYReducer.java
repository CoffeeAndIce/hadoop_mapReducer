package hadoop.DIYSort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hadoop.Writable.Serialization.KpiWritable;


public class DIYReducer extends Reducer<KpiWritable, Text, KpiWritable, NullWritable>{

	@Override
	protected void reduce(KpiWritable arg0, Iterable<Text> arg1,
			Reducer<KpiWritable, Text, KpiWritable, NullWritable>.Context arg2) throws IOException, InterruptedException {
		
	}
	
}
