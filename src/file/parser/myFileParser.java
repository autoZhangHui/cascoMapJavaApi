package file.parser;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class myFileParser {
	//parse txtfiles
	public static void readTxtFile(String filePath){
        try {
                String encoding="GBK";
                File file=new File(filePath);
                if(file.isFile() && file.exists()){ //判断文件是否存在
                    InputStreamReader read = new InputStreamReader(
                    new FileInputStream(file),encoding);//考虑到编码格式
                    BufferedReader bufferedReader = new BufferedReader(read);
                    String lineTxt = null;
                    String rowkey=null;
                    String data=null;
                    String headInfo=null;
                    String dataInfo=null;
                    while((lineTxt = bufferedReader.readLine()) != null){
                    	headInfo=lineTxt.substring(0,lineTxt.indexOf(':'));
                    	dataInfo=lineTxt.substring(lineTxt.indexOf(':')+1,lineTxt.length());
                        System.out.println("datahead:"+headInfo);
                        if(dataInfo.equals("ff"))
                        	System.out.println("记录数据错误");
                        else
                        	System.out.println("datahead:"+dataInfo);
                        	
                    }
                    read.close();
		        }else{
		            System.out.println("找不到指定的文件");
		        }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
	}
	
	public static void main(String[] agrs) throws Exception  {
		readTxtFile("/Users/zhanghui/Downloads/1.txt");
	}
}

