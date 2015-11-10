package bigdata_test;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;

public class PutIntoHbase {
	public static class HbaseData {
		String dataName;
		String dataType;
		String dataDate;
		String dataValue;
		String dataSrc;
		//dataSrc="as";
		HbaseData(){
			dataName="asasda";
			dataType="ana";
			dataDate="ana";
			dataValue="ana";
			dataSrc="ana";
		}
	}
	public static String getLogsName(String dataFileName)
	{
		
		return null;
	}
	public static String readLogs(String logFileName)
	{
		return null;
	}
	public static void normalizeData(String dataFileName,String logFileName)
	{
		
	}
	private static void putIntoHbase(String[] data)
	{
		
	}
	public static void main(String[] args)throws Exception{
		
		HbaseData mydata1 = new HbaseData();
		mydata1.dataDate="11a";
		mydata1.dataSrc="11ws";
		try{
			System.out.println(mydata1.dataDate);
		}catch(Exception e){
			
		}
		
	}
}

