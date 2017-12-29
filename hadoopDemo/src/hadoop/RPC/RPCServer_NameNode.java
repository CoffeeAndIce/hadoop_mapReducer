package hadoop.RPC;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer_NameNode implements ClientProtocal{

		public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
			Server server = new RPC.Builder(new Configuration()).setInstance(new RPCServer_NameNode()).setProtocol(ClientProtocal.class).setBindAddress("192.168.1.212").setPort(9876).build();
			server.start();
		}

		public String findMetaDataByName(String filename){
			System.out.println("正在从内存中找"+filename+"的元数据信息");
			return filename+"找到后元数据信息";
		}
	}
