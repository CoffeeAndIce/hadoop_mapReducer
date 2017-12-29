package hadoop.progress;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import hadoop.Writable.Deserialization.MyKpiJob.KpiWritable;

public class KpiPartitioner extends Partitioner<Text, KpiWritable> {
    @Override
    public int getPartition(Text key, KpiWritable value, int numPartitions) {
        // 实现不同的长度不同的号码分配到不同的reduce task中，返回的数值从0开始算节点数
        String substring = key.toString().substring(0, 3);
        if (substring.equals("135")) {
            return 0;
        } else {
            return 1;
        }
    }
}
