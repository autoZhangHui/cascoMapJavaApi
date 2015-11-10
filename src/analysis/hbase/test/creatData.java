package analysis.hbase.test;

import bigdata_test.SetHbase;

public class creatData {
	//buzhengchang
	 for(int year=2011;year<=2015;year++)
	   {
		   for(int month=1;month<=12;month++){
			   for(int hour=0;hour<24;hour++){
				   for(int minute=0;minute<60;minute++){
					   rowkey="shanghai:3G:gddy:";
					   if(month<10){
						   rowkey=rowkey+year+"0"+month+date+":"+hour+":"+minute;
					   }else{
						   rowkey=rowkey+year+month+date+":"+hour+":"+minute;
					   }
					  
						   ackSecond=Integer.toString((int)(3*Math.random()+21-0.0001*count));
						   SetHbase.addRecord(table, rowkey, familys[0], familys[0], "1");
						   SetHbase.addRecord(table, rowkey, familys[1], familys[1], ackSecond);
						   count++;
				   }
			   }
		   }
	   }
	 
	 for(int year=2011;year<=2015;year++)
	   {
		   for(int month=1;month<=12;month++){
			   for(int hour=0;hour<24;hour++){
				   for(int minute=0;minute<60;minute++){
					   rowkey="shanghai:3G:gddy:";
					   if(month<10){
						   rowkey=rowkey+year+"0"+month+date+":"+hour+":"+minute;
					   }else{
						   rowkey=rowkey+year+month+date+":"+hour+":"+minute;
					   }
					  
						   ackSecond=Integer.toString((int)(3*Math.random()+21));
						   SetHbase.addRecord(table, rowkey, familys[0], familys[0], "1");
						   SetHbase.addRecord(table, rowkey, familys[1], familys[1], ackSecond);
						   count++;
				   }
			   }
		   }
	   }
}
