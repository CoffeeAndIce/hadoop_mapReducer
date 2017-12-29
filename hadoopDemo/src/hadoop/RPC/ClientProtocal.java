package hadoop.RPC;


/**
* @ClassName: ClientProtocal
* @Description:协议模拟
* @author linge
* @date 2017年12月26日 下午6:28:53
*
*/
public interface ClientProtocal {
	
	public static final long versionID=123123l;
	
	public String findMetaDataByName(String filename);
}
