package edu.buffalo.cse562.berkelydb;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class DatabaseManager {
	
	private Environment myDbEnvironment = null;
	private EnvironmentConfig envConfig;
	private Map<String,Database> tableMap = new HashMap<>();
	
	public DatabaseManager(String envHome){
		envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    // Open the environment. Create it if it does not already exist.
	    myDbEnvironment = new Environment(new File(envHome),envConfig);
	}
	
	public void createIndexedTable(String tableName){
		
		Database myDatabase = null;
		try {
		    // Open the database. Create it if it does not already exist.
		    DatabaseConfig dbConfig = new DatabaseConfig();
		    dbConfig.setAllowCreate(true);
		    // Make it deferred write
		    dbConfig.setDeferredWrite(true);
		    myDatabase = myDbEnvironment.openDatabase(null, tableName, dbConfig); 
		    // do work
		    
		    // Do this when you want the work to be persistent at a
		    // specific point, prior to closing the database.
		    myDatabase.sync();
		    // then close the database and environment here
		    // (described later in this chapter).
		    tableMap.put(tableName, myDatabase);
		} catch (DatabaseException dbe) {
		    // Exception handling goes here
		}
	}
	
	public Database getTable(String tableName){
		if(!tableMap.containsKey(tableName)){
			// Open the database. Create it if it does not already exist.
		    DatabaseConfig dbConfig = new DatabaseConfig();
		    dbConfig.setAllowCreate(true);
		    // Make it deferred write
		    dbConfig.setDeferredWrite(true);
		    tableMap.put(tableName,myDbEnvironment.openDatabase(null, tableName, dbConfig));
		}
		return tableMap.get(tableName);
	}
	
	public void close(){
		Iterator<String> iterator = tableMap.keySet().iterator();
		while(iterator.hasNext()){
			tableMap.get(iterator.next()).close();
		}
		myDbEnvironment.close();
	}
}
