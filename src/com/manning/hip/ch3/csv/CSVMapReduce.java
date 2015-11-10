package com.manning.hip.ch3.csv;


import bigdata_test.SetHbase;

import com.google.protobuf.DescriptorProtos.EnumOptions;
import com.manning.hip.ch3.TextArrayWritable;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.text.AbstractDocument.Content;



public final class CSVMapReduce {
  public static int maxKeyValue;
  public static String[] totalAline= new String[4];
 // public static String[] keywords=new String[2];
  public static List<String> keywords=new ArrayList<String>();
  public static List<String> colunms= new ArrayList<String>();
  public static String startDate;
  public static String endDate;
  private static Configuration conf =null;
  static {  
      conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum","hadoop,slave1,slave2,slave3");
  } 
 
  private static Text[] convert(String[] s) {
	  Text t[] = new Text[s.length];
	  for(int i=0; i < t.length; i++) {
	  t[i] = new Text(s[i]);
	  //System.out.println(s[i]);
	  }
	  return t;
	  //System.out.println(t[]);
  }
  public static String resultToJson(String targetTable,String basicRowName) throws Exception{
	  HTable table = new HTable(conf, targetTable);
	  String[] familys={"context","average"};
	  String rowName="";
	  String temp="";
	  ArrayList<String> dataString=new <String>ArrayList();
	  ArrayList<String> keywordsin=new <String>ArrayList();
	  basicRowName="logs:shanghai:OMAS:"+startDate+":";
	  Get g=null;
	  Result rowresult=null;
	  JSONObject resultJson=new JSONObject();
	  rowName=basicRowName+"total";
	  g=new Get(Bytes.toBytes(rowName));
	  rowresult=table.get(g);
	  byte[] value=rowresult.getValue(familys[0].getBytes(),familys[0].getBytes());
	  //System.out.println( new String(value));
	  
	  try{
		  resultJson.put("title", new String(value));
		  for(int i=1;i<100;i++){
			  rowName=basicRowName+i;
			  g=new Get(Bytes.toBytes(rowName));
			  rowresult=table.get(g);
			  value=rowresult.getValue(familys[0].getBytes(),familys[0].getBytes());
			  temp=new String(value);
			  //System.out.println("temp first"+temp);
			  String[] temps=temp.split(";");
			  dataString.add(temps[0]+";"+temps[1]+";"+temps[2]);
			  temp="";
			  for(int j=3;j<temps.length;j++){
				  temp+=temps[j]+";";
				  //System.out.println(temps[j]);
			  }
			  keywordsin.add(temp);
			  
		  }
		  
	  } catch(Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  resultJson.put("data",dataString.toArray());
	  resultJson.put("keywords",keywordsin.toArray());
	  resultJson.put("ext_total",dataString.toArray().length);
	  resultJson.put("total",maxKeyValue);
	  return resultJson.toString();
  }
  private static String textToString(Text[] input){
	  String output="";
	  for(int i=0;i<input.length;i++){
		  output+=input[i].toString();
	  }
	  return output;
  }
  private static void findKeywordPos(Text[] lineContent,Object[] objects){
	  String startWord="File name";
	  int keywordNum=objects.length;
	  int lineContentNum=lineContent.length;
	  if(lineContent[0].toString().equals(startWord)){
		  for(int i=0;i<lineContentNum;i++){
			  for(int j=0;j<keywordNum;j++){
				 
				  if(lineContent[i].toString().equals(objects[j].toString())){
					  System.out.println(objects[j].toString());
					  //keywordPos[0].name=keywords[0];
					  colunms.add(String.valueOf(i));
					  
				  }
			  }
			 
		  }
		  //System.out.println(lineContent[0].toString()+colunms[0]+colunms[1]);
	  }
	  
  }
  public static class Map       //<co id="ch03_comment_csv_mr1"/>
      extends Mapper<LongWritable, TextArrayWritable,
      LongWritable, TextArrayWritable> {

    @Override
    protected void map(LongWritable key, TextArrayWritable value,
                       Context context)
        throws
        IOException, InterruptedException {
    	
    	Text[] lineContent=(Text[]) value.toArray();
    	int contentNum=lineContent.length;
    	String[] valueString=new String[keywords.toArray().length];
    	for(int i=0;i<keywords.toArray().length;i++){
    		valueString[i]=keywords.toArray()[i].toString();
    	}
    	String[] valueText=new String[3+2*keywords.toArray().length];
    	
    	//System.out.println(valueText.length+"keywordsleng"+keywords.toArray().length);
    	//colunms.toArray()[0];
    	//System.out.println(colunms.toArray()[0]);
    	if(colunms.toArray().length==0){
    		findKeywordPos(lineContent,keywords.toArray());
    		//System.out.println(colunms[0]);
    	}else{
        	if(contentNum>26){
        		valueText[0]=lineContent[0].toString()+";";
        		valueText[1]=lineContent[1].toString()+";";
        		valueText[2]=lineContent[2].toString()+";";
        		int ss=0;//count valueSrtring
        		for(int j=3;j<valueText.length;j++){
        			valueText[j]=valueString[ss]+":";
        			j++;
//        			System.out.println(j+"linecontent"+lineContent.length);
//        			System.out.println(colunms.toArray().length+"ss"+ss+"columms"+colunms.toArray()[ss]);
//        			System.out.println(lineContent[Integer.parseInt(colunms.toArray()[ss].toString())]);
            		valueText[j]=lineContent[Integer.parseInt(colunms.toArray()[ss].toString())].toString()+";";
//            		System.out.println(j);
            		ss++;
        		}
        	}else{
        		valueText[0]="haha! just a joke";
        		valueText[1]="haha! just a joke";
        		valueText[2]="haha! just a joke";
        		valueText[3]="haha! just a joke";
            	valueText[4]="haha! just a joke";
            	valueText[5]="haha! just a joke";
            	valueText[6]="haha! just a joke";
        	}
        	
        	TextArrayWritable comText=new TextArrayWritable(convert(valueText));
        	context.write(key, comText);
        	int keyNum=(int)key.get();
        	if(keyNum>=maxKeyValue){
        		 maxKeyValue++;
        	 }
    	}	
    }
  }

  public static class Reduce     //<co id="ch03_comment_csv_mr2"/>
      extends Reducer<LongWritable, TextArrayWritable,
      TextArrayWritable, NullWritable> {
	private IntWritable resultNum=new IntWritable();
	int countUnusal=0;
	int reduceCount=0;
	String targetTable="AnalysisResult";
	String rowkey="logs"+":"+"shanghai:"+"OMAS"+"20151020";
    String[] familys={"context","average"};
    public void reduce(LongWritable key,
                       Iterable<TextArrayWritable> values,
                       Context context)
        throws IOException, InterruptedException {
    	  reduceCount++;
    	  
    	  HTable table = new HTable(conf, targetTable);
    	  for(TextArrayWritable val : values){
    		  Text[] lineContent=(Text[]) val.toArray();
    		  int contentNum=lineContent.length;
    		  int judgeFlag=0;
    		  int valueCount=(contentNum-3)/2;
    		  for(int i=0;i<valueCount;i++){
    			  int actualCount=i*2+4;
    			  //System.out.print(lineContent[actualCount].toString());
    			  if(lineContent[actualCount].toString().equals("True;")){
    				  judgeFlag++;
    			  }
    		  }
    		  //System.out.print(judgeFlag+"saddsad"+valueCount);
    		  if(judgeFlag>0){
    			  context.write(val, NullWritable.get());
    			  //System.out.println(lineContent[6].toString());
    			  countUnusal++;
    			  rowkey="logs"+":"+"shanghai:"+"OMAS:"+startDate+":"+countUnusal;
                try {
                	//System.out.print(lineContent);
  					SetHbase.addRecord(table, rowkey, familys[0], familys[0], textToString(lineContent));
  				} catch (Exception e) {
  					// TODO Auto-generated catch block
  					e.printStackTrace();
  				}
    			  //System.out.println(countUnusal);
    		  }
    		  //System.out.println(reduceCount+"aaa"+maxKeyValue);
    		  if(maxKeyValue==reduceCount){
    			  //System.out.println(countUnusal);
    			  totalAline[0]=startDate+"-"+endDate;
    			  totalAline[1]="此期间OMAS日志一共发现";
    			  totalAline[2]=resultNum+"";
    			  totalAline[3]="次异常";
    			  rowkey="logs"+":"+"shanghai:"+"OMAS:"+startDate+":"+"total";
                  try {
    					SetHbase.addRecord(table, rowkey, familys[0], familys[0], textToString(convert(totalAline)));
    				} catch (Exception e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    			  TextArrayWritable finalText=new TextArrayWritable(convert(totalAline));
    			  context.write(finalText, NullWritable.get());
    		  }
    		  resultNum.set(countUnusal);
    	  }
    	
    	  
    }
  }
public static String runLogs(String[] inkeywords,String startTime,String endTime) throws Exception{
	HConnectionManager.deleteConnection(conf, true);
	conf.set("hbase.zookeeper.quorum","hadoop,slave1,slave2,slave3");
	for(int i=0;i<inkeywords.length;i++){
		keywords.add(inkeywords[i]);
	}
	startDate=startTime;
	endDate=endTime;
	 String sad1= "hdfs://hadoop:9000/user/hadoop/input/1.csv";   
	 String sad2="hdfs://hadoop:9000/user/eclipse/files/output/1.txt";
	 runJob(sad1,sad2,"");
	 String ss=resultToJson("AnalysisResult","");
	 return ss;
}
  public static void main(String... args) throws Exception {
	String[] in=new String[args.length-2];
	  for(int i=2;i<args.length;i++){
		//keywords.add(args[i]);
		in[i-2]=args[i];
	}
	
	//runLogs(in);
	 // System.out.println(in);
   String ss=runLogs(in,"20150720","20150830");
     System.out.println(ss);
    //runJob(args[0], args[1],args[2]);
    
  }

  public static void runJob(String input,
                            String output,
                            String anakeyword)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");  //<co id="ch03_comment_csv_mr3"/>
    conf.set(CSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ":"); //<co id="ch03_comment_csv_mr4"/>

    Job job = new Job(conf);
    job.setJarByClass(CSVMapReduce.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(CSVInputFormat.class); //<co id="ch03_comment_csv_mr5"/>
    job.setOutputFormatClass(CSVOutputFormat.class); //<co id="ch03_comment_csv_mr6"/>

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(TextArrayWritable.class);

    job.setOutputKeyClass(TextArrayWritable.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    Path outPath = new Path(output);
    FileOutputFormat.setOutputPath(job, outPath);
    outPath.getFileSystem(conf).delete(outPath, true);

    job.waitForCompletion(true);
  }
}