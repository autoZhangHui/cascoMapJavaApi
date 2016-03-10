package input.data.hbase;
 
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import analysis.hbase.test.HbaseSet.Reduce;
 
public class inputDataHbase {
   private static Configuration conf =null;
   public static String stationInfo;
   static {  
       conf = HBaseConfiguration.create();
       conf.set("hbase.zookeeper.quorum","hadoop,slave1,slave2,slave3");
   }  
     
    static class ImportMapper extends Mapper<LongWritable, Text, Text, Text>{
         int valueNum=0;
         String keyString;
         String valueString;
         Text outKey= new Text();
         Text outValue=new Text();
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
        	String[] mapValue=value.toString().trim().split(":");
        	valueNum=mapValue.length;
        	//System.out.println(valueNum);
        	if(valueNum>=5){
        		keyString=stationInfo+":"+mapValue[1]+":"+mapValue[2]+":"+mapValue[3];
        		valueString=mapValue[4]+":"+mapValue[5];
        		outKey.set(keyString);
        		outValue.set(valueString);
        		System.out.println(keyString);
        		context.write(outKey, outValue);
        	}
        	
        };
    }
     
    static class ImportReducer extends TableReducer<Text, Text, NullWritable>{
        protected void reduce(Text key, Iterable<Text> values,    Context context) throws java.io.IOException ,InterruptedException {
            String tableKey=key.toString();
        	for (Text text : values) {
                final String[] splited = text.toString().split(":");
                 
                final Put put = new Put(Bytes.toBytes(tableKey));
                put.add(Bytes.toBytes("value"), Bytes.toBytes("value"), Bytes.toBytes(splited[1]));
                put.add(Bytes.toBytes("seconds"), Bytes.toBytes("seconds"+splited[0]), Bytes.toBytes(splited[0]));
                //省略其他字段，调用put.add(....)即可
                context.write(NullWritable.get(), put);
            }
        };
    }
    public static void creatTable(String tableName, String[] familys) throws Exception {     
        HBaseAdmin admin = new HBaseAdmin(conf);     
        if (admin.tableExists(tableName)) {     
            System.out.println("table already exists!");     
        } else {     
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);     
            for(int i=0; i<familys.length; i++){     
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));     
            }     
            admin.createTable(tableDesc);     
            System.out.println("create table " + tableName + " ok.");     
        }      
    } 
    public static String findTableName(String inputPath) throws FileNotFoundException, IOException{
    	FileInputStream fis=new FileInputStream(inputPath);
    	 String line="";
         String[] arrs=null;
         String[] outputString= new String[4];
    	if(fis!=null){
    		InputStreamReader isr=new InputStreamReader(fis, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            //简写如下
            //BufferedReader br = new BufferedReader(new InputStreamReader(
            //        new FileInp"wlan_log"utStream("E:/phsftp/evdokey/evdokey_201103221556.txt"), "UTF-8"));
           
            for(int i=0;i<3;i++){
            	line=br.readLine();
                arrs=line.split(":");
                outputString[i]=arrs[1];
                System.out.println(arrs[0] + " : " + arrs[1]);
            }
            outputString[3]=outputString[0]+outputString[1];
            stationInfo=outputString[0]+":"+outputString[2]+":";
            System.out.println(outputString[2]);
            br.close();
            isr.close();
            fis.close();
    	}
    	return outputString[3];
    }
    public static void main(String[] args) throws Exception {
         String inputPath=args[0];
         String outTableName;
         String[] familys={"seconds","value"};
         outTableName=findTableName(inputPath);
         creatTable(outTableName,familys);
        //设置hbase表名称
        conf.set(TableOutputFormat.OUTPUT_TABLE, outTableName);
        //将该值改大，防止hbase超时退出
        conf.set("dfs.socket.timeout", "180000");
         
        final Job job = new Job(conf, "ImportDataFromFiles");
        job.setJarByClass(inputDataHbase.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(ImportMapper.class);
        job.setReducerClass(ImportReducer.class);
        //设置map的输出，不设置reduce的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class); 
        FileInputFormat.setInputPaths(job, new Path(inputPath)); 
        System.exit(job.waitForCompletion(true)?0:1);
    }
}