package edu.buffalo.cse562.berkelydb;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.berkelydb.lineitem.LineItemPrimaryKeyBinding;
import edu.buffalo.cse562.datasource.DataSourceReader;
import edu.buffalo.cse562.datasource.FileDataSource;
import edu.buffalo.cse562.utils.TableUtils;

public class DatabaseManager {
	
	private Environment myDbEnvironment = null;
	private EnvironmentConfig envConfig;
	private DatabaseConfig dbConfig = new DatabaseConfig();
	private Map<String,Database> openPrimaryDatabases = new HashMap<>();
	private Map<String,SecondaryDatabase> openSecondaryDatabases = new HashMap<>();
	
	public DatabaseManager(String envHome){
		envConfig = new EnvironmentConfig();
		envConfig.setSharedCache(true);
		envConfig.setConfigParam("je.log.fileMax", "100000000");
	    envConfig.setAllowCreate(true);
	    envConfig.setLocking(false);
	    // Open the environment. Create it if it does not already exist.
	    myDbEnvironment = new Environment(new File(envHome),envConfig);
	    // Open the database. Create it if it does not already exist.
	    dbConfig.setAllowCreate(true);
	    // Make it deferred write
	    dbConfig.setDeferredWrite(true);
	}
	
	public SecondaryDatabase createSecondaryIndexedTable(Database primaryDatabase,String tableName,SecondaryKeyCreator secondaryKey){
		SecondaryConfig mySecConfig = new SecondaryConfig();
		mySecConfig.setAllowCreate(true);
		mySecConfig.setAllowPopulate(true);
		mySecConfig.setSortedDuplicates(true);
		mySecConfig.setKeyCreator(secondaryKey);
		mySecConfig.setDeferredWrite(true);
		SecondaryDatabase db =
				myDbEnvironment.openSecondaryDatabase(
	                    null,     
	                    tableName, // Index name
	                    primaryDatabase,     // Primary database handle. This is
	                                     // the db that we're indexing. 
	                    mySecConfig);    // The secondary config
		db.sync();
		openSecondaryDatabases.put(tableName,db);
		return db;
	}
	
	public Database createIndexedTable(String indexName,int indexPosition, File sourceData,TupleBinding<LeafValue[]> binding,List<ColumnDefinition> colDefns){		
		Database myDatabase = null;
		try {
		    myDatabase = myDbEnvironment.openDatabase(null, indexName, dbConfig); 
		    // start indexing table
		    FileDataSource source = new FileDataSource(sourceData, colDefns);
			DataSourceReader reader =  source.getReader();
			TupleBinding<LeafValue[]> pKeyBinding = new LineItemPrimaryKeyBinding();
			String tableName = indexName.split("\\.")[0];
			boolean lineItemTable = tableName.equals("LINEITEM");
			LeafValue[] row = null;
			while( (row = reader.readNextTuple())!=null){
				DatabaseEntry tuple = new DatabaseEntry();
				DatabaseEntry key = new DatabaseEntry();
				binding.objectToEntry(row, tuple);
				if(lineItemTable){
					pKeyBinding.objectToEntry(row, key);
				}else{
					TableUtils.bindLeafValueToKey(row[indexPosition], key);
				}
				//TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(row[0].toLong(), key);
				myDatabase.put(null, key, tuple);
			}
			myDatabase.sync();
			openPrimaryDatabases.put(indexName, myDatabase);
			return myDatabase;
		} catch (Exception dbe) {
			throw new RuntimeException(dbe);
		}
	}

    public Database getPrimaryDatabase(String tableName){
    	if(!openPrimaryDatabases.containsKey(tableName)){
    		 DatabaseConfig dbConfig = new DatabaseConfig();
    		 dbConfig.setReadOnly(true);
    		 dbConfig.setSortedDuplicates(false);
    		 dbConfig.setTransactional(false);
    		 openPrimaryDatabases.put(tableName,myDbEnvironment.openDatabase(null, tableName, dbConfig));
    	}
        return openPrimaryDatabases.get(tableName);
    }

    public SecondaryDatabase getSecondaryDatabase(Database primaryDatabase,String secondaryIndexName,SecondaryKeyCreator secondaryKey){
    	if(!openSecondaryDatabases.containsKey(secondaryIndexName)){
    		SecondaryConfig dbConfig  = new SecondaryConfig();
            dbConfig.setKeyCreator(secondaryKey);
            dbConfig.setReadOnly(true);
            dbConfig.setSortedDuplicates(true);
            dbConfig.setTransactional(false);
            openSecondaryDatabases.put(secondaryIndexName, myDbEnvironment.openSecondaryDatabase(null, secondaryIndexName, primaryDatabase, dbConfig));
    	}
        return openSecondaryDatabases.get(secondaryIndexName);
    }
	
//	public LeafValue[] lookupPrimaryIndex(String primaryIndex,LeafValue leafValue){
//		DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
//		Database customer =manager.getPrimaryIndex(primaryIndex);
//		DatabaseEntry key = new DatabaseEntry();
//		TableUtils.bindLeafValueToKey(leafValue, key);
//		DatabaseEntry tuple = new DatabaseEntry();
//		customer.get(null, key, tuple, LockMode.READ_UNCOMMITTED);
//		CustomerLeafValueBinding binding = new CustomerLeafValueBinding();
//		LeafValue[] results = binding.entryToObject(tuple);
//		return results;
//	}
//
//	public void lookupSecondaryIndexes(String indexName,LeafValue value,TupleBinding<LeafValue[]> binding,SecondaryDatabase secondaryDb){
//		long start = System.currentTimeMillis();
//		DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
//		DatabaseEntry key = new DatabaseEntry();
//		DatabaseEntry pkey = new DatabaseEntry();
//		TableUtils.bindLeafValueToKey(value, key);
//		DatabaseEntry tuple = new DatabaseEntry();
//		SecondaryCursor cursor = secondaryDb.openSecondaryCursor(null, new CursorConfig());
//		cursor.getCurrent(null, null, LockMode.READ_UNCOMMITTED);
//		OperationStatus returnVal = cursor.getSearchKey(key, pkey,tuple, LockMode.READ_UNCOMMITTED);
//		while(returnVal== OperationStatus.SUCCESS){
//			LeafValue[] results = binding.entryToObject(tuple);
//			for(int i=0;i<results.length;i++)
//				System.out.print(results[i].toString()+ " ");
//			System.out.println();
//			returnVal = cursor.getNextDup(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
//		}
//		System.out.println("Time Taken"+(System.currentTimeMillis() - start));
//	}
	
	public void close(){
		closeDB(openSecondaryDatabases);
		closeDB(openPrimaryDatabases);
		myDbEnvironment.close();
	}
	
	private void closeDB(Map<String,? extends Database> tableMap){
		Iterator<String> iterator = tableMap.keySet().iterator();
		while(iterator.hasNext()){
			tableMap.get(iterator.next()).close();
		}
	}
	private void closeDB(List<Database> databases){
		Iterator<Database> iterator = databases.iterator();
		while(iterator.hasNext()){
			iterator.next().close();
		}
	}
}
