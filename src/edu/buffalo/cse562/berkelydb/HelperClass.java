package edu.buffalo.cse562.berkelydb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.dbi.RangeConstraint;
import com.sleepycat.util.keyrange.RangeCursor;

import edu.buffalo.cse562.berkelydb.customer.CustomerLeafValueBinding;

public class HelperClass {

	public static void main(String[] args) throws Exception{
		indexTable();
		long start = System.currentTimeMillis();
		createPhoneIndex();
		System.out.println("Time Taken to build secondary Index"+(System.currentTimeMillis() - start));
		lookup();
		lookupSecondaryIndexes(null, null, null);
		lookupSecondaryIndexes(null, null, null);
	}
	
	private static void lookupSecondaryIndexes(String indexName,LeafValue value,TupleBinding<LeafValue[]> tupleBinding){
		long start = System.currentTimeMillis();
		DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
		SecondaryKeyCreaterImpl phoneKey = new SecondaryKeyCreaterImpl(new CustomerLeafValueBinding(), 4);
		SecondaryDatabase customer =manager.getSecondaryIndex(manager.getPrimaryIndex("customer"), "custphone", phoneKey);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry pkey = new DatabaseEntry();
		TupleBinding.getPrimitiveBinding(String.class).objectToEntry("21-120-483-4861", key);
		DatabaseEntry tuple = new DatabaseEntry();
		SecondaryCursor cursor = customer.openSecondaryCursor(null, new CursorConfig());
		CustomerLeafValueBinding binding = new CustomerLeafValueBinding();
		cursor.getCurrent(null, null, LockMode.READ_UNCOMMITTED);
		OperationStatus returnVal = cursor.getSearchKey(key, pkey,tuple, LockMode.READ_UNCOMMITTED);
		while(returnVal== OperationStatus.SUCCESS){
			LeafValue[] results = binding.entryToObject(tuple);
			for(int i=0;i<results.length;i++)
				System.out.print(results[i].toString()+ " ");
			System.out.println();
			returnVal = cursor.getNextDup(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
		}
		System.out.println("Time Taken"+(System.currentTimeMillis() - start));
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
		long start = System.currentTimeMillis();
		manager.createIndexedTable("customer", file, new CustomerLeafValueBinding(), colDefns);
		System.out.println("Time Taken to Index"+(System.currentTimeMillis() - start));
		manager.close();
	}
	
	private static void createPhoneIndex(){
		DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
		SecondaryKeyCreaterImpl phoneKey = new SecondaryKeyCreaterImpl(new CustomerLeafValueBinding(), 4);
		manager.createSecondaryIndexedTable(manager.getPrimaryIndex("customer"), "custphone", phoneKey);
		manager.close();
	}
	
	private static void lookup(){
		long start = System.currentTimeMillis();
		DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
		Database customer =manager.getPrimaryIndex("customer");
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
