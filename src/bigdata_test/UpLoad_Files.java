package bigdata_test;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/*
 * this file is to upload datafiles to hdfs and record it.
 */
import org.apache.hadoop.util.Progressable;
import org.vafer.jdeb.shaded.compress.compress.utils.IOUtils;


public class UpLoad_Files {
	public static void load_hdfs(String input_pos,String output_pos) throws Exception{
		//String local_pos="/home/hadoop/files/";
		String local_pos=input_pos;
		String hdfs_pos=output_pos;
		Configuration conf = new Configuration(); //启用默认配置
		InputStream in=new BufferedInputStream(new FileInputStream(local_pos));
		FileSystem fs= FileSystem.get(URI.create(hdfs_pos), conf);
		OutputStream out=fs.create(new Path(hdfs_pos),new Progressable(){
			public void progress(){
				System.out.print("*");
			}
		});
		IOUtils.copy(in, out, 4096);		
	}
	public static void main(String[] args) throws Exception{
		String input_pos="/home/hadoop/casco_bigdata/hadoop/input/file01";
		String output_pos="hdfs://hadoop:9000/user/eclipse/files/file02";
		load_hdfs(input_pos,output_pos);
	}
}
