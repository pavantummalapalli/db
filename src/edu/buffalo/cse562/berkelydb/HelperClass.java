package edu.buffalo.cse562.berkelydb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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

import edu.buffalo.cse562.berkelydb.customer.CustomerLeafValueBinding;
import edu.buffalo.cse562.berkelydb.orders.OrdersLeafValueBinding;
import edu.buffalo.cse562.datasource.DataSourceReader;
import edu.buffalo.cse562.datasource.FileDataSource;

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
		SecondaryDatabase customer =manager.getSecondaryIndex("custphone");
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
		manager.createIndexedTable("customer", 0,file, new CustomerLeafValueBinding(), colDefns);
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
    private static void cksTest() throws Exception {
        //Index Build

        File file = new File("/home/cksharma/data/git/db-prjs/dbgen/sf5/orders.csv");
        List<ColumnDefinition> colDefns = new ArrayList<>();
        colDefns.add(generateColumnDef("a", "int"));
        colDefns.add(generateColumnDef("b", "int"));
        colDefns.add(generateColumnDef("c", "char"));
        colDefns.add(generateColumnDef("d", "decimal"));
        colDefns.add(generateColumnDef("e", "date"));
        colDefns.add(generateColumnDef("f", "char"));
        colDefns.add(generateColumnDef("g", "char"));
        colDefns.add(generateColumnDef("h", "int"));
        colDefns.add(generateColumnDef("i", "char"));
        DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
        TupleBinding<LeafValue[]> binding = new OrdersLeafValueBinding();
        FileDataSource source = new FileDataSource(file, colDefns);
        manager.createIndexedTable("orders", 0,source.getFile(), binding, colDefns);
        Database orders =  manager.getPrimaryIndex("orders");
        DataSourceReader reader =  source.getReader();
        LeafValue[] row = null;
        long start = System.currentTimeMillis();
        while( (row = reader.readNextTuple())!=null){
            DatabaseEntry tuple = new DatabaseEntry();
            binding.objectToEntry(row, tuple);
            DatabaseEntry key = new DatabaseEntry();
            TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(row[0].toLong(), key);
            orders.put(null, key, tuple);
        }
        orders.sync();
        System.out.println("Time Taken to Index"+(System.currentTimeMillis() - start));
        manager.close();

        //index lookup

        start = System.currentTimeMillis();
        manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
        orders =manager.getPrimaryIndex("orders");
        DatabaseEntry key = new DatabaseEntry();
        TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(327L, key);
        DatabaseEntry tuple = new DatabaseEntry();
        orders.get(null, key, tuple, LockMode.READ_UNCOMMITTED);
        System.out.println("Time Taken"+(System.currentTimeMillis() - start));
        LeafValue[] results = binding.entryToObject(tuple);
        for(int i=0;i<results.length;i++)
            System.out.println(results[i].toString());
    }
}
