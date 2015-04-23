package edu.buffalo.cse562.berkelydb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;

import edu.buffalo.cse562.berkelydb.customer.CustomerLeafValueBinding;
import edu.buffalo.cse562.datasource.DataSourceReader;
import edu.buffalo.cse562.queryplan.FileDataSource;

public class HelperClass {

	public static void main(String[] args) throws Exception{
//		indexTable();
		lookup();
	}
	
	private static void indexTable() throws Exception{
		File file = new File("E:\\workspace\\java\\db\\data\\sf1\\customer.csv");
		List<ColumnDefinition> colDefns = new ArrayList<>();
		colDefns.add(generateColumnDef("a", "int"));
		colDefns.add(generateColumnDef("b", "char"));
		colDefns.add(generateColumnDef("c", "char"));
		colDefns.add(generateColumnDef("d", "int"));
		colDefns.add(generateColumnDef("e", "char"));
		colDefns.add(generateColumnDef("f", "decimal"));
		colDefns.add(generateColumnDef("g", "char"));
		colDefns.add(generateColumnDef("h", "char"));
		DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
		manager.createIndexedTable("customer");
		Database customer =  manager.getTable("customer");
		FileDataSource source = new FileDataSource(file, colDefns);
		DataSourceReader reader =  source.getReader();
		LeafValue[] row = null;
		long start = System.currentTimeMillis();
		while( (row = reader.readNextTuple())!=null){
			CustomerLeafValueBinding binding = new CustomerLeafValueBinding();
			DatabaseEntry tuple = new DatabaseEntry();
			binding.objectToEntry(row, tuple);
			DatabaseEntry key = new DatabaseEntry();
			TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(row[0].toLong(), key);
			customer.put(null, key, tuple);
		}
		customer.sync();
		System.out.println("Time Taken to Index"+(System.currentTimeMillis() - start));
		manager.close();
	}
	
	private static void lookup(){
		long start = System.currentTimeMillis();
		DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
		Database customer =manager.getTable("customer");
		DatabaseEntry key = new DatabaseEntry();
		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(149991L, key);
		DatabaseEntry tuple = new DatabaseEntry();
		customer.get(null, key, tuple, LockMode.READ_UNCOMMITTED);
		System.out.println("Time Taken"+(System.currentTimeMillis() - start));
		CustomerLeafValueBinding binding = new CustomerLeafValueBinding();
		LeafValue[] results = binding.entryToObject(tuple);
		for(int i=0;i<results.length;i++)
			System.out.println(results[i].toString());
	}
	
	private static ColumnDefinition generateColumnDef(String columnName,String dataType){
		ColumnDefinition def = new ColumnDefinition();
		ColDataType type = new ColDataType();
		type.setDataType(dataType);
		def.setColDataType(type);
		def.setColumnName(columnName);
		return def;
	}
}
