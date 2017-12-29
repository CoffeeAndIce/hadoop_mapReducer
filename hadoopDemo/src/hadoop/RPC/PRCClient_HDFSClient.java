package hadoop.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class PRCClient_HDFSClient {

	public static void main(String[] args) throws IOException {
		ClientProtocal proxy = RPC.getProxy(ClientProtocal.class, 123123l, new InetSocketAddress("192.168.1.212", 9876), new Configuration());
		String findMetaDataByName = proxy.findMetaDataByName("468");
		System.out.println(findMetaDataByName);
		
		String findMetaDataByName2 = proxy.findMetaDataByName("/ha.txt");
		System.out.println(findMetaDataByName2);
		
		RPC.stopProxy(proxy);
	}

}
