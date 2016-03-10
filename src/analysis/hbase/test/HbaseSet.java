package analysis.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.ws.rs.PUT;

import net.sf.json.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import com.manning.hip.ch3.TextArrayWritable;

import bigdata_test.SetHbase;

public class HbaseSet {
	private static Configuration conf = null;
	public static int startDate = 0;
	public static int endDate = 0;
	public static String selectDevice = "";
	public static String selectVar = "";
	/**
	 * 初始化配置
	 */
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop,slave1,slave2,slave3");
	}

	public static class TestStudent {
		int sas = 1;
		int dsad = 2;
	}

	/**
	 * 创建一张表
	 */
	public static class myMapper extends TableMapper<Text, IntWritable> {
		/*
		 * 按行读取habse对应表中的行值，提取成需要的数据格式
		 */
		public String sqlDate;
		public int sqlDateint;
		public int sqlValueint;
		public String sqlMonth;
		public String sqlYear;
		public String splitQuto = ":";
		public byte[] test;
		public byte[] columnNameByte;
		public String columnName = "value";
		public String sqlValue;
		private IntWritable intvalue = new IntWritable();
		private Text outputRowkey = new Text();
		String[] sqlData;

		@Override
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			sqlData = value.list().get(1).toString().split(splitQuto);

			// System.out.println(Arrays.toString(test));
			// System.out.println(new String(test,"UTF-8"));
			sqlDate = sqlData[3];
			sqlDateint = Integer.parseInt(sqlDate);
			if (selectVar.equals(sqlData[2]) && selectDevice.equals(sqlData[1])
					&& sqlDateint >= startDate && sqlDateint <= endDate) {
				sqlYear = sqlDate.substring(0, 6);
				columnNameByte = Bytes.toBytes(columnName);
				test = value.getValue(columnNameByte, columnNameByte);

				sqlValue = new String(test, "UTF-8");
				sqlValueint = Integer.parseInt(sqlValue);
				// System.out.println(sqlYear);
				intvalue.set(sqlValueint);
				// sqlMonth=sqlDate.substring(4, 6);
				sqlYear = sqlData[0] + ":" + sqlData[1] + ":" + sqlData[2]
						+ ":" + sqlYear;
				outputRowkey.set(sqlYear);
				context.write(outputRowkey, intvalue);
			}

		}
	}

	// reduce类需要定义成static类否则会报init错误
	public static class Reduce extends
			TableReducer<Text, IntWritable, NullWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int average = 0;
			int counts = 0;
			for (IntWritable i : values) {
				sum += i.get();
				counts++;
				// System.out.println(sum);
			}
			average = sum / counts;
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("context"), Bytes.toBytes("average"),
					Bytes.toBytes(String.valueOf(average)));
			context.write(NullWritable.get(), put);
		}
	}

	public static void creatTable(String tableName, String[] familys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
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
	public void addRecord(HTable table, String rowKey, String family,
			String qualifier, String value) throws Exception {
		try {
			// HTable table = new HTable(conf,
			// tableName);如果动态插入record需要注意不能够在循环中创建大对象否则可能出现内存溢出错误。
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ "tableName" + " ok.");
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除一行记录
	 */
	public static void delRecord(String tableName, String rowKey)
			throws IOException {
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
	public static void getOneRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			// System.out.print(new String(kv.getRow()) + " " );
			// System.out.print(new String(kv.getFamily()) + ":" );
			// System.out.print(new String(kv.getQualifier()) + " " );
			// System.out.print(kv.getTimestamp() + " " );
			// System.out.println(new String(kv.getValue()));
		}
	}

	public static void getAllRecord(String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					// System.out.print(new String(kv.getRow()) + " ");
					// System.out.print(new String(kv.getFamily()) + ":");
					// System.out.print(new String(kv.getQualifier()) + " ");
					// System.out.print(kv.getTimestamp() + " ");
					// System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 显示所有数据
	 */
	public static void getAllRecord1(String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					// System.out.print(new String(kv.getRow()) + " ");
					// System.out.print(new String(kv.getFamily()) + ":");
					// System.out.print(new String(kv.getQualifier()) + " ");
					// System.out.print(kv.getTimestamp() + " ");
					// System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getAllHistory(String tableName) {
		ReturnResult result = new ReturnResult();
		JSONObject resultJson = new JSONObject();
		JSONArray restArray = new JSONArray();
		String finalString = "";
		try {

			tableName = "AnalysisHistory";
			String[] familys = { "value" };
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {

					// System.out.print(new String(kv.getRow()) + " ");
					resultJson.put("time", new String(kv.getRow()));
					resultJson.put("data", new String(kv.getValue()));
					restArray.add(resultJson);
					// System.out.print(new String(kv.getFa)
					// System.out.print(new String(kv.getFamily()) + ":");
					// System.out.print(new String(kv.getQualifier()) + " ");
					// System.out.print(kv.getTimestamp() + " ");
					// System.out.println(new String(kv.getValue()));
				}
			}

			// System.out.println(resultjson);
			finalString = restArray.toString();
			// System.out.println(finalString);

		} catch (Exception e) {

		}
		return finalString;
	}

	public static void addHistory(String tableName) {
		try {
			tableName = "AnalysisHistory";
			String[] familys = { "value" };
			SetHbase.creatTable(tableName, familys);
			// HTable table = new HTable(conf, tableName);
			// SetHbase.addRecord(table,"20100101:20150830:shanghai:dxtx:1G",familys[0],familys[0],"just have a try!");
		} catch (Exception e) {

		}

	}

	public static void initHbase() {

		try {
			// String[] tablename =
			// {"FileInfo","StationDevice","DeviceBool","DeviceAna","DeviceCircle"};
			// String[] familys = {"pos", "logs","lognum"};
			// String fileRowkey="1.txt";
			// SetHbase.creatTable(tablename[0], familys);
			// System.out.println(tablename);
			// HTable table = new HTable(conf, tablename[0]);
			// //add record zkb
			// SetHbase.addRecord(table,fileRowkey,"pos","pos1","/user/eclipse/files/1");
			// SetHbase.addRecord(table,fileRowkey,"logs","logs"+"1","2.logs");
			// SetHbase.addRecord(table,fileRowkey,"logs","lognum","1");
			// String[] familysStation={"device"};
			// fileRowkey="/华东/上海虹桥/";
			// table = new HTable(conf, tablename[1]);
			// SetHbase.creatTable(tablename[1], familysStation);
			// SetHbase.addRecord(table,fileRowkey,"device","device","机柜1");
			// String[] familysBool={"time","value"};
			// fileRowkey="shanghai_hongqiao_jigui1";
			// SetHbase.creatTable(tablename[2], familysBool);
			// table = new HTable(conf, tablename[2]);
			// SetHbase.addRecord(table,fileRowkey,"time","time"+"1","2015.06.30.05:30");
			// SetHbase.addRecord(table,fileRowkey,"value","value"+"1","0");
			// String[] familysAna={"time","value"};
			// fileRowkey="shanghai_hongqiao_jigui1";
			// table = new HTable(conf, tablename[3]);
			// SetHbase.creatTable(tablename[3], familysAna);
			// SetHbase.addRecord(table,fileRowkey,"time","time"+"1","2015.06.30.05:30");
			// SetHbase.addRecord(table,fileRowkey,"value","value"+"1","0101001");
			// String[] familysCircle={"time","value"};
			// fileRowkey="shanghai_hongqiao_jigui1";
			// SetHbase.creatTable(tablename[4], familysCircle);
			// table = new HTable(conf, tablename[4]);
			// SetHbase.addRecord(table,fileRowkey,"time","time"+"1","2015.06.30.05:30");
			// SetHbase.addRecord(table,fileRowkey,"value","value"+"1","0101001");
			// System.out.println("===========get one record========");
			// SetHbase.getOneRecord(tablename[1], fileRowkey);
			// System.out.println("===========show all record========");
			// SetHbase.getAllRecord(tablename[0]);
			String[] TableName = { "station_total", "shanghai_ana_201601",
					"shanghai_20160101" };
			String[] Table1_familys = { "info", "var_info", "logic_info" };
			String[] Table2_familys = { "value" };
			String[] Table3_familys = { "value" };
			// SetHbase.creatTable(TableName[0], Table1_familys);
			// SetHbase.creatTable(TableName[1], Table2_familys);
			// SetHbase.creatTable(TableName[2], Table3_familys);
			HTable table = new HTable(conf, TableName[1]);
			SetHbase.addRecord(
					table,
					"dc:cdbs:5DG:20:20:18",
					Table2_familys[0],
					"roal_value",
					"20;15;12;18;25;18;16;30;25;23;20;15;12;18;25;18;16;30;25;23;20;15;12;18;25;18;16;30;25;23;");
			SetHbase.addRecord(table, "dc:cdbs:5DG:20:20:18",
					Table2_familys[0], "error", "0");
			SetHbase.addRecord(table, "dc:cdbs:5DG:20:20:18",
					Table2_familys[0], "time_detail", "2016/01/18/20:08");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void setHbaseData(String tableName, String rowkey)
			throws Exception {
		tableName = "ddhl";
		String[] familys = { "seconds", "value" };
		String ackSecond;
		SetHbase.creatTable(tableName, familys);
		HTable table = new HTable(conf, tableName);
		// rowkey="shanghai:device1:20150630";
		int date = 15;
		int count = 0;

		for (int year = 2011; year <= 2015; year++) {
			for (int month = 1; month <= 12; month++) {
				for (int dateInt = 10; dateInt <= 10; dateInt++) {
					for (int hour = 0; hour < 24; hour++) {
						for (int minute = 0; minute < 60; minute++) {
							rowkey = "ddhl:3G:gddy:";
							if (month < 10) {
								rowkey = rowkey + year + "0" + month + dateInt
										+ ":" + hour + ":" + minute;
							} else {
								rowkey = rowkey + year + month + date + ":"
										+ hour + ":" + minute;
							}

							ackSecond = Integer.toString((int) (3 * Math
									.random() + 19));
							SetHbase.addRecord(table, rowkey, familys[0],
									familys[0], "1");
							SetHbase.addRecord(table, rowkey, familys[1],
									familys[1], ackSecond);
							count++;
						}
					}
				}

			}
		}
	}

	public static void setErrorData(String tableName, String rowkey)
			throws Exception {
		tableName = "ddhl";
		String[] familys = { "seconds", "value" };
		String ackSecond;
		SetHbase.creatTable(tableName, familys);
		HTable table = new HTable(conf, tableName);
		// rowkey="shanghai:device1:20150630";
		int date = 15;
		int count = 0;
		int year = 2014;
		int month = 06;

		for (int dateInt = 10; dateInt <= 10; dateInt++) {
			for (int hour = 0; hour < 24; hour++) {
				for (int minute = 0; minute < 60; minute++) {
					rowkey = "ddhl:3#:dwzlbs:";
					if (month < 10) {
						rowkey = rowkey + year + "0" + month + dateInt + ":"
								+ hour + ":" + minute;
					} else {
						rowkey = rowkey + year + month + date + ":" + hour
								+ ":" + minute;
					}

					ackSecond = Integer
							.toString((int) (3 * Math.random() + 13));
					SetHbase.addRecord(table, rowkey, familys[0], familys[0],
							"1");
					SetHbase.addRecord(table, rowkey, familys[1], familys[1],
							ackSecond);
					count++;
				}
			}
		}

	}

	public static void setError1Data(String tableName, String rowkey)
			throws Exception {
		tableName = "ddhl";
		String[] familys = { "seconds", "value" };
		String ackSecond;
		SetHbase.creatTable(tableName, familys);
		HTable table = new HTable(conf, tableName);
		// rowkey="shanghai:device1:20150630";
		int date = 15;
		int count = 0;
		double l = 25;
		for (int year = 2011; year <= 2015; year++) {
			for (int month = 1; month <= 12; month++) {
				l = l - 0.15;
				for (int dateInt = 10; dateInt <= 10; dateInt++) {
					for (int hour = 0; hour < 24; hour++) {
						for (int minute = 0; minute < 60; minute++) {
							rowkey = "ddhl:4G:gddy:";
							if (month < 10) {
								rowkey = rowkey + year + "0" + month + dateInt
										+ ":" + hour + ":" + minute;
							} else {
								rowkey = rowkey + year + month + date + ":"
										+ hour + ":" + minute;
							}

							ackSecond = Integer.toString((int) (1 * Math
									.random() + l));
							SetHbase.addRecord(table, rowkey, familys[0],
									familys[0], "1");
							SetHbase.addRecord(table, rowkey, familys[1],
									familys[1], ackSecond);
							count++;
						}
					}
				}

			}
		}
	}

	public static String outTitle(String station, String device,
			String typeVar, Boolean is_bj) throws Exception {
		String stationName = "";
		String varName = "";
		String title = "";
		if (station.equals("rmgc")) {
			stationName = "人民广场站";
		} else if (station.equals("jwtyc")) {
			stationName = "江湾体育场站";
		} else if (station.equals("wjc")) {
			stationName = "五角场站";
		} else if (station.equals("xjh")) {
			stationName = "徐家汇站";
		} else if (station.equals("shhcz")) {
			stationName = "上海火车站站";
		} else if (station.equals("ddhl")) {
			stationName = "大渡河路站";
		} else if (station.equals("jyl")) {
			stationName = "金运路站";
		} else if (station.equals("fz")) {
			stationName = "丰庄站";
		} else {
			stationName = station;
		}
		if (typeVar.equals("gddy")) {
			varName = "轨道电压";
		} else if (typeVar.equals("dwzlbs")) {
			varName = "定位直流表示电压";
		} else if (typeVar.equals("dwjlbs")) {
			varName = "定位交流表示电压";
		} else if (typeVar.equals("fwzlbs")) {
			varName = "反位直流表示电压";
		} else if (typeVar.equals("fwjlbs")) {
			varName = "反位交流表示电压";
		} else if (typeVar.equals("gdxwj")) {
			varName = "轨道相位角";
		} else if (typeVar.equals("scdy")) {
			varName = "输出电压";
		} else {
			varName = typeVar;
		}
		if (is_bj) {
			title = stationName + device + varName + "异常";
		} else {
			title = stationName + device + varName + "正常";
		}

		return title;
	}

	public static String outDsp(String station, String device, String typeVar,
			String startTime, String endTime, String index, Boolean is_bj)
			throws Exception {
		String stationName = "";
		String varName = "";
		String dsp = "";
		if (station.equals("rmgc")) {
			stationName = "人民广场站";
		} else if (station.equals("jwtyc")) {
			stationName = "江湾体育场站";
		} else if (station.equals("wjc")) {
			stationName = "五角场站";
		} else if (station.equals("xjh")) {
			stationName = "徐家汇站";
		} else if (station.equals("shhcz")) {
			stationName = "上海火车站站";
		} else if (station.equals("ddhl")) {
			stationName = "大渡河路站";
		} else if (station.equals("jyl")) {
			stationName = "金运路站";
		} else if (station.equals("fz")) {
			stationName = "丰庄站";
		} else {
			stationName = station;
		}
		if (typeVar.equals("gddy")) {
			varName = "轨道电压";
		} else if (typeVar.equals("dwzlbs")) {
			varName = "定位直流表示电压";
		} else if (typeVar.equals("dwjlbs")) {
			varName = "定位交流表示电压";
		} else if (typeVar.equals("fwzlbs")) {
			varName = "反位直流表示电压";
		} else if (typeVar.equals("fwjlbs")) {
			varName = "反位交流表示电压";
		} else if (typeVar.equals("gdxwj")) {
			varName = "轨道相位角";
		} else if (typeVar.equals("scdy")) {
			varName = "输出电压";
		} else {
			varName = typeVar;
		}
		if (typeVar.equals("gddy")) {
			if (is_bj) {
				dsp = "从" + startTime + "到" + endTime + "时间段分析中，" + stationName
						+ "的设备" + device + "的" + varName + "发生异常，具体情况是"
						+ varName + "趋势下降过快，下降率为" + index + "伏/月，可能发生危险。";
			} else {
				dsp = "从" + startTime + "到" + endTime + "时间段分析中，" + stationName
						+ "的设备" + device + "的" + varName + "未发生报警。";
			}

		} else if (typeVar.equals("dwzlbs")) {
			if (is_bj) {
				dsp = "从" + startTime + "到" + endTime + "时间段分析中，" + stationName
						+ "的设备" + device + "的" + varName + "发生异常，具体情况是"
						+ varName + index.substring(0, index.length() - 1)
						+ "月监测值异常" + "，可能发生危险。";
			} else {
				dsp = "从" + startTime + "到" + endTime + "时间段分析中，" + stationName
						+ "的设备" + device + "的" + varName + "未发生报警。";
			}

		} else {
			if (is_bj) {
				dsp = "从" + startTime + "到" + endTime + "时间段分析中，" + stationName
						+ "的设备" + device + "的" + varName + "发生错误报警,可能发生危险。";
			} else {
				dsp = "从" + startTime + "到" + endTime + "时间段分析中，" + stationName
						+ "的设备" + device + "的" + varName + "未发生报警。";
			}

		}
		return dsp;
	}

	public static JSONObject resulttojson(String targetTable, String station,
			String device, String typeVar, String startDate, String endDate)
			throws Exception {
		HTable table = new HTable(conf, targetTable);
		String[] familys = { "context", "average" };
		String startYStr = startDate.substring(0, 4);
		String endYStr = endDate.substring(0, 4);
		String startMStr = startDate.substring(4, 6);
		String endMStr = endDate.substring(4, 6);
		String row = null;
		Get g = null;
		Result rowresult = null;
		int startYInt = Integer.parseInt(startYStr);
		int startMInt = Integer.parseInt(startMStr);
		int endYInt = Integer.parseInt(endYStr);
		int endMInt = Integer.parseInt(endMStr);
		ReturnResult result = new ReturnResult();
		result.stationName = targetTable;
		for (int y = startYInt; y <= endYInt; y++) {
			if (y == startYInt) {
				for (int m = startMInt; m <= 12; m++) {
					row = station + ":" + device + ":" + typeVar + ":";
					if (0 < m && m < 10) {

						row = row + y + "0" + m;
						result.date.add(row.split(":")[3]);
						g = new Get(Bytes.toBytes(row));
						rowresult = table.get(g);
						byte[] value = rowresult.getValue(
								familys[0].getBytes(), familys[1].getBytes());
						result.data.add(new String(value));
					} else {
						row = row + y + m;
						result.date.add(row.split(":")[3]);
						g = new Get(Bytes.toBytes(row));
						rowresult = table.get(g);
						byte[] value = rowresult.getValue(
								familys[0].getBytes(), familys[1].getBytes());
						// String as=new String(value);
						result.data.add(new String(value));
					}

				}

			} else if (y == endYInt) {
				for (int m = 1; m <= endMInt; m++) {
					row = station + ":" + device + ":" + typeVar + ":";
					if (0 < m && m < 10) {
						row = row + y + "0" + m;
						result.date.add(row.split(":")[3]);
						g = new Get(Bytes.toBytes(row));
						rowresult = table.get(g);
						byte[] value = rowresult.getValue(
								familys[0].getBytes(), familys[1].getBytes());
						result.data.add(new String(value));
					} else {
						row = row + y + m;
						result.date.add(row.split(":")[3]);
						g = new Get(Bytes.toBytes(row));
						rowresult = table.get(g);
						byte[] value = rowresult.getValue(
								familys[0].getBytes(), familys[1].getBytes());
						// String as=new String(value);
						result.data.add(new String(value));
					}
				}
			} else {
				for (int m = 1; m <= 12; m++) {
					row = station + ":" + device + ":" + typeVar + ":";
					if (0 < m && m < 10) {
						row = row + y + "0" + m;
						result.date.add(row.split(":")[3]);
						g = new Get(Bytes.toBytes(row));
						rowresult = table.get(g);
						byte[] value = rowresult.getValue(
								familys[0].getBytes(), familys[1].getBytes());
						result.data.add(new String(value));
					} else {
						row = row + y + m;
						result.date.add(row.split(":")[3]);
						g = new Get(Bytes.toBytes(row));
						rowresult = table.get(g);
						byte[] value = rowresult.getValue(
								familys[0].getBytes(), familys[1].getBytes());
						// String as=new String(value);
						result.data.add(new String(value));
					}
				}
			}
		}
		double[] doubleIn = new double[result.data.toArray().length];
		for (int i = 0; i < doubleIn.length; i++) {
			doubleIn[i] = Double.parseDouble((String) result.data.toArray()[i]);
		}
		String[] stringIn = new String[result.data.toArray().length];
		for (int i = 0; i < stringIn.length; i++) {
			stringIn[i] = (String) result.date.toArray()[i];
		}
		String anaResult = "";
		double anaDouble = 0;
		boolean Is_bj = false;
		if (typeVar.equals("gddy")) {
			anaDouble = analysisMath.calculateYbyX(doubleIn);
			anaDouble = (double) (Math.round(anaDouble * 100)) / 100;
			anaDouble = Math.abs(anaDouble);
		} else if (typeVar.equals("dwzlbs")) {
			anaResult = analysisMath.calculateUsual(doubleIn, stringIn) + "";
		} else {
			anaResult = "";
		}

		if (!anaResult.equals("")) {
			Is_bj = true;
		} else if (Math.abs(anaDouble) > 0.05) {
			Is_bj = true;
		} else {
			Is_bj = false;
		}

		if (typeVar.equals("gddy")) {
			anaResult = anaDouble + "";
		}
		JSONObject resultJson = new JSONObject();
		resultJson.put("name", result.stationName);
		resultJson.put("station", station);
		resultJson.put("device", device);
		resultJson.put("typeVar", typeVar);
		resultJson.put("is_bj", Is_bj);
		resultJson.put("title", outTitle(station, device, typeVar, Is_bj));
		resultJson.put(
				"dsp",
				outDsp(station, device, typeVar, startDate, endDate, anaResult,
						Is_bj));
		resultJson.put("date", result.date);
		resultJson.put("data", result.data);
		// System.out.println(resultjson);
		String finalString = resultJson.toString();
		System.out.println(finalString);
		return resultJson;
	}

	public static String finalReturn(String[] anaObjects, String targetTable,
			String startTime, String endTime, String sbType) throws Exception {
		String stationName;
		String device;
		String typeVar;
		String tableName = targetTable;
		ArrayList myJson = new<JSONObject> ArrayList();
		for (int i = 0; i < anaObjects.length; i++) {
			stationName = anaObjects[i].split(":")[0];
			device = anaObjects[i].split(":")[1];
			typeVar = anaObjects[i].split(":")[2];
			myJson.add(resulttojson(tableName, stationName, device, typeVar,
					startTime, endTime));
		}
		HTable tableHis = new HTable(conf, "AnalysisHistory");
		String hisRow = sbType + "分析时间段为：" + startDate + "到" + endDate;
		SetHbase.addRecord(tableHis, hisRow, "value", "value",
				myJson.toString());
		return myJson.toString();
	}

	@SuppressWarnings("deprecation")
	public static void runJob(String[] analysisObj) throws Exception {
		// String inTableName="shanghaidata";
		// String targetTable="AnalysisResult";//设定输入输出表具体内容待定
		String inTableName = analysisObj[0];
		String targetTable = analysisObj[1];
		Job job = new Job(conf, "ExampleSummaryToFile");
		// System.out.println(1);
		Scan scan = new Scan();

		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		job.setJarByClass(HbaseSet.class);
		TableMapReduceUtil.initTableMapperJob(inTableName, scan,
				myMapper.class, ImmutableBytesWritable.class, Put.class, job);
		// initmap之后还可以通过设定mapout重新设定map输出，不然会报错。
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		TableMapReduceUtil.initTableReducerJob(targetTable, // output table
				Reduce.class, // reducer class
				job);
		job.waitForCompletion(true);
	}

	public static String runHbase(String[] anaTables, String targetTable,
			String startTime, String endTime, String sbType) throws Exception {
		HConnectionManager.deleteConnection(conf, true);
		conf.set("hbase.zookeeper.quorum", "hadoop,slave1,slave2,slave3");
		// HconnectioImplement
		String[] tableName = { "haha", "asd" };
		tableName[1] = targetTable;
		startDate = Integer.parseInt(startTime);
		endDate = Integer.parseInt(endTime);
		String stationTable = new String();
		String devices = new String();
		for (int i = 0; i < anaTables.length; i++) {
			stationTable = anaTables[i].split(":")[0];
			devices = anaTables[i].split(":")[1];
			selectDevice = devices;
			selectVar = anaTables[i].split(":")[2];
			tableName[0] = stationTable;
			runJob(tableName);
		}
		String finalString = "";
		finalString = finalReturn(anaTables, targetTable, startTime, endTime,
				sbType);
		// HConnection myconnection=new HConnection(conf);
		return finalString;

	}

	public static void main(String[] agrs) throws Exception {
		// String tableName="hsah";
		String[] tableName = { "haha", "asd" };
		tableName[0] = agrs[0];
		tableName[1] = agrs[1];
		startDate = Integer.parseInt(agrs[2]);
		endDate = Integer.parseInt(agrs[3]);
		String[] test = { "ddhl:1#:dwzlbs" };
		// String
		// testss=runHbase(test,"AnalysisResult","20110101","20150930","道岔");
		// System.out.print(testss);
		for (int i = 4; i < agrs.length; i++) {
			selectDevice = agrs[i];
			// runJob(tableName);
		}
		// selectDevice=
		// String rowkey="haha";
		// String startdate="20150601";
		// String enddate="20150731";
		// startDate=Integer.parseInt(startdate);
		// endDate=Integer.parseInt(enddate);
		// addHistory("");
		// String sss=getAllHistory("");
		// System.out.print(sss);
		setHbaseData("sad", "sad");
		setError1Data("", "");
		// setErrorData("","");
		// initHbase();
		// String
		// webreturn=resulttojson(tableName[1],"shenyang","3G","gddy",agrs[2],agrs[3]);
		// return null;
	}
}
