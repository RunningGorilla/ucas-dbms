import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;

/** 
* Reading data from HDFS,then make data dinstinct using Hashtable structure, finally save data to HBase
* @author yangxiaolin
* @institue cnic
* @date 2019/4/1 
*/
public class Hw1Grp4
{
	public static HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();

	public static void main(String[] args) 
	{
		//if no input any
		if (args.length <= 0)
		{
			System.out.println("Please Inter the args!"); //error
			System.exit(1); // exit
		}
		
		// args[0]:get file name
		String fileName = args[0].replace("R=", ""); 
		
		// args[1]:calculation type
		String[] type = args[1].replace("select:", "").split("\\,");
		int column = Integer.parseInt(type[0].replace("R","")); // which column calculated
		String op = type[1]; // 6 kind of calculation type 
		double value = Double.parseDouble(type[2]); // witch value
		
		//result:args[2]
		String[] resIndex = args[2].replace("distinct:", "").split("\\,");
		int lenIndex = resIndex.length;
		
		try
		{
			MidRes mres = dataFromHDFS(fileName);

			BufferedReader br = mres.getBR();
			distinct2HashMap(br, column, op, value, resIndex, lenIndex);
			mres.getIn().close();
			mres.getFS().close();

			save2HBase(resIndex, lenIndex);

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

/**
 * Read data from HDFS then return the  MidRes object
 * @param filename the FileName of table in the HDFS 
 */
	public static MidRes dataFromHDFS(String fileName) throws URISyntaxException, IOException
	{
		String absAdr = "hdfs://localhost:9000" + fileName;
		//config HDFS
        Configuration config = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(absAdr), config); 
        Path path = new Path(absAdr); 
        FSDataInputStream in_stream = fs.open(path); 
		
		//file output to Buffer Pool
        BufferedReader br = new BufferedReader(new InputStreamReader(in_stream));
		MidRes mres = new MidRes(br, fs, in_stream);
		return mres;
	}
	
/**
 * use HashMap to Temporarily store data then make data dintinct
 * @param br the BufferedReader object return from function dataFromHDFS
 * @param column the index of the selected column
 * @param op the operator for comparison
 * @param value the value for comparison
 * @param resInd the array saving the index of selected columns
 * @param lenIndex total number of selected columns
 */
	public static void distinct2HashMap(BufferedReader br, int column, String op, double value, String[] resInd, int lenIndex)
		throws IOException
	{
		int[] resIndex = new int[lenIndex];
		for (int i = 0; i < lenIndex; ++i )
			resIndex[i] = Integer.parseInt(resInd[i].replace("R", ""));
		String line = null; 
		while ((line = br.readLine()) != null)
		{
			String[] slice = line.split("\\|");
			boolean flag = false;
			//gt:greater than
			if (op.equals("gt") && (Double.parseDouble(slice[column]) > value))
				flag = true;
			//ge:greater equal
			else if (op.equals("ge") && (Double.parseDouble(slice[column]) >= value))
				flag = true;
			//eq:equal
			else if (op.equals("eq") && (Double.parseDouble(slice[column]) == value))
				flag = true;
			//ne: not equal
			else if (op.equals("ne") && (Double.parseDouble(slice[column]) != value))
				flag = true;
			// le: less equal
			else if (op.equals("le") && (Double.parseDouble(slice[column]) <= value))
				flag = true;
			// lt: less than
			else if(op.equals("lt") && (Double.parseDouble(slice[column]) < value))
				flag = true;

			if (flag == true)
			{
				StringBuffer key = new StringBuffer("");
				ArrayList<String> al = new ArrayList<String>();
				for (int i = 0; i < lenIndex; i++)
				{
					key.append(slice[resIndex[i]]);
					al.add(slice[resIndex[i]]);
				}
				if (hm.containsKey(key.toString())) continue;
				hm.put(key.toString(), al);
			}
		}
	}

/**
 * save data to HBase
 * @param resIndex The array saving selected colnums index
 * @param lenIndex the array length
 */
	public static void save2HBase(String[] resIndex, int lenIndex) throws IOException, MasterNotRunningException,ZooKeeperConnectionException
	{
		Logger.getRootLogger().setLevel(Level.WARN); 
		String tableName = "Result";
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor cf = new HColumnDescriptor("res");
		htd.addFamily(cf);
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin hAdmin = new HBaseAdmin(config);
		if (hAdmin.tableExists(tableName))
		{	
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
			System.out.println("Table already exists");
			System.out.println("Previous table have been deleted!");
		}
		else
		{
			hAdmin.createTable(htd);
			System.out.println("table" + tableName + "created succeessfully");
		}
		hAdmin.close();

		HTable table = new HTable(config,tableName);
		int count = 0;
		Iterator it = hm.entrySet().iterator(); 
		while (it.hasNext())
		{
			Map.Entry entry = (Map.Entry)it.next();
			String key = (String)entry.getKey();
			ArrayList<String> al = (ArrayList<String>)entry.getValue();
			Put put = new Put(String.valueOf(count).getBytes());
			for (int i = 0;i < lenIndex;++i ){
				put.add("res".getBytes(),resIndex[i].getBytes(), al.get(i).getBytes());
				table.put(put);	
			}
			count++;
		}
		table.close();
		System.out.println("complete successfully!");
	}
}

/** 
* save FileSystem and BufferedReader object for convience 
*/
class MidRes
{
	private BufferedReader br = null;
	private FileSystem fs = null;
	private FSDataInputStream in_stream = null;

	public MidRes(BufferedReader br, FileSystem fs, FSDataInputStream in_stream)
	{
		this.br = br;
		this.fs = fs;
		this.in_stream = in_stream;
	}
/** 
* return  BufferedReader object for safe
*/
	public BufferedReader getBR()
	{
		return br;
	}
/** 
* return FileSystem object for safe
*/
	public FileSystem getFS()
	{
		return fs;
	}
/** 
* return FSDataInputStream object for safe
*/
	public FSDataInputStream getIn()
	{
		return in_stream;
	}
}
	

