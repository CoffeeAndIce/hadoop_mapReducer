package hadoop.Writable.Serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
* @ClassName: KpiWritable
* @Description: 封装KpiWritable类型
* @author linge
* @date 2017年12月27日 下午5:12:18
*
*/
public class KpiWritable implements Writable{
	private long upPackNum;     // 上行数据包数，单位：个
	private long downPackNum;  // 下行数据包数，单位：个
	private long upPayLoad;    // 上行总流量，单位：byte
	private long downPayLoad;  // 下行总流量，单位：byte
	//方便序列化
	public KpiWritable() {
		super();
	}
	
    public KpiWritable(long upPackNum, long downPackNum, long upPayLoad, long downPayLoad) {
		super();
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
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

	public long getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(long upPackNum) {
		this.upPackNum = upPackNum;
	}

	public long getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(long downPackNum) {
		this.downPackNum = downPackNum;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}
	
	
}
