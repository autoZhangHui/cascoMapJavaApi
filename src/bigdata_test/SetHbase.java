package bigdata_test;
/*
 * This file is to inifinal hbase.
 */
import java.io.IOException;     
import java.util.ArrayList;     
import java.util.List;     
      
import org.apache.hadoop.conf.Configuration;     
import org.apache.hadoop.hbase.HBaseConfiguration;     
import org.apache.hadoop.hbase.HColumnDescriptor;     
import org.apache.hadoop.hbase.HTableDescriptor;     
import org.apache.hadoop.hbase.KeyValue;     
import org.apache.hadoop.hbase.MasterNotRunningException;     
import org.apache.hadoop.hbase.ZooKeeperConnectionException;     
import org.apache.hadoop.hbase.client.Delete;     
import org.apache.hadoop.hbase.client.Get;     
import org.apache.hadoop.hbase.client.HBaseAdmin;     
import org.apache.hadoop.hbase.client.HTable;     
import org.apache.hadoop.hbase.client.Result;     
import org.apache.hadoop.hbase.client.ResultScanner;     
import org.apache.hadoop.hbase.client.Scan;     
import org.apache.hadoop.hbase.client.Put;     
import org.apache.hadoop.hbase.util.Bytes;   
public class SetHbase {
	 private static Configuration conf =null;
	 
     /** 
      * 初始化配置 
     */  
     static {  
         conf = HBaseConfiguration.create();
         conf.set("hbase.zookeeper.quorum","hadoop,slave1,slave2,slave3");
     }  
       
    /**   
     * 创建一张表   
     */    
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
         
    /**   
     * 删除表   
     */    
    public static void deleteTable(String tableName) throws Exception {     
       try {     
           HBaseAdmin admin = new HBaseAdmin(conf);     
           admin.disableTable(tableName);     
           admin.deleteTable(tableName);     
           System.out.println("delete table " + tableName + " ok.");     
       } catch (MasterNotRunningException e) {     
           e.printStackTrace();     
       } catch (ZooKeeperConnectionException e) {     
           e.printStackTrace();     
       }     
    }     
          
    /**   
     * 插入一行记录   
     */    
    public static void addRecord (HTable table, String rowKey, String family, String qualifier, String value)     
            throws Exception{     
        try {     
            //HTable table = new HTable(conf, table);     
            Put put = new Put(Bytes.toBytes(rowKey));     
            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));     
            table.put(put);     
            System.out.println("insert recored " + rowKey + " to table " + table +" ok.");     
        } catch (IOException e) {     
            e.printStackTrace();     
        }     
    }     
      
    /**   
     * 删除一行记录   
     */    
    public static void delRecord (String tableName, String rowKey) throws IOException{     
        HTable table = new HTable(conf, tableName);     
        List<Delete> list = new ArrayList<Delete>();     
        Delete del = new Delete(rowKey.getBytes());     
        list.add(del);     
        table.delete(list);     
        System.out.println("del recored " + rowKey + " ok.");     
    }     
          
    /**   
     * 查找一行记录   
     */    
    public static void getOneRecord (String tableName, String rowKey) throws IOException
    {     
        HTable table = new HTable(conf, tableName);     
        Get get = new Get(rowKey.getBytes());     
        Result rs = table.get(get);     
        for(KeyValue kv : rs.raw()){     
            System.out.print(new String(kv.getRow()) + " " );     
            System.out.print(new String(kv.getFamily()) + ":" );     
            System.out.print(new String(kv.getQualifier()) + " " );     
            System.out.print(kv.getTimestamp() + " " );     
            System.out.println(new String(kv.getValue()));      
        }
    }
            
    public static void getAllRecord (String tableName) {     
                try{     
                    HTable table = new HTable(conf, tableName);     
                    Scan s = new Scan();     
                    ResultScanner ss = table.getScanner(s);     
                    for(Result r:ss){     
                        for(KeyValue kv : r.raw()){     
                           System.out.print(new String(kv.getRow()) + " ");     
                           System.out.print(new String(kv.getFamily()) + ":");     
                           System.out.print(new String(kv.getQualifier()) + " ");     
                           System.out.print(kv.getTimestamp() + " ");     
                           System.out.println(new String(kv.getValue()));     
                        }     
                    }     
               } catch (IOException e){     
                   e.printStackTrace();     
               }     
        }     
    
          
    /**   
     * 显示所有数据   
     */    
    public static void getAllRecord1 (String tableName) {     
        try{     
             HTable table = new HTable(conf, tableName);     
             Scan s = new Scan();     
             ResultScanner ss = table.getScanner(s);     
             for(Result r:ss){     
                 for(KeyValue kv : r.raw()){     
                    System.out.print(new String(kv.getRow()) + " ");     
                    System.out.print(new String(kv.getFamily()) + ":");     
                    System.out.print(new String(kv.getQualifier()) + " ");     
                    System.out.print(kv.getTimestamp() + " ");     
                    System.out.println(new String(kv.getValue()));     
                 }     
             }     
        } catch (IOException e){     
            e.printStackTrace();     
        }     
    }     
         
    public static void  main (String [] agrs) {     
        try {     
            String[] tablename = {"FileInfo","StationDevice","DeviceBool","DeviceAna","DeviceCircle"};     
            String[] familys = {"pos", "logs","lognum"};
            String fileRowkey="1.txt";
            SetHbase.creatTable(tablename[0], familys);     
            System.out.println(tablename);
            //add record zkb     
           // SetHbase.addRecord(tablename[0],fileRowkey,"pos","pos1","/user/eclipse/files/1");        
            //SetHbase.addRecord(tablename[0],fileRowkey,"logs","logs"+"1","2.logs");
            //SetHbase.addRecord(tablename[0],fileRowkey,"logs","lognum","1");
            String[] familysStation={"device"};
            fileRowkey="/华东/上海虹桥/";
            SetHbase.creatTable(tablename[1], familysStation);
            //SetHbase.addRecord(tablename[1],fileRowkey,"device","device","机柜1");
            String[] familysBool={"time","value"};
            fileRowkey="shanghai_hongqiao_jigui1";
            SetHbase.creatTable(tablename[2], familysBool);
            //SetHbase.addRecord(tablename[2],fileRowkey,"time","time"+"1","2015.06.30.05:30");
            //SetHbase.addRecord(tablename[2],fileRowkey,"value","value"+"1","0");
            String[] familysAna={"time","value"};
            fileRowkey="shanghai_hongqiao_jigui1";
            SetHbase.creatTable(tablename[3], familysAna);
            //SetHbase.addRecord(tablename[3],fileRowkey,"time","time"+"1","2015.06.30.05:30");
            //SetHbase.addRecord(tablename[3],fileRowkey,"value","value"+"1","0101001");
            String[] familysCircle={"time","value"};
            fileRowkey="shanghai_hongqiao_jigui1";
            SetHbase.creatTable(tablename[4], familysCircle);
//            SetHbase.addRecord(tablename[4],fileRowkey,"time","time"+"1","2015.06.30.05:30");
//            SetHbase.addRecord(tablename[4],fileRowkey,"value","value"+"1","0101001");   
            System.out.println("===========get one record========");     
            SetHbase.getOneRecord(tablename[1], fileRowkey);                      
            System.out.println("===========show all record========");     
            SetHbase.getAllRecord(tablename[0]);     
        } catch (Exception e) {     
            e.printStackTrace();     
        }     
    }     
}
