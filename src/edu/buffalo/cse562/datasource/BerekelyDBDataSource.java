package edu.buffalo.cse562.datasource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

import edu.buffalo.cse562.ExpressionTriplets;
import edu.buffalo.cse562.berkelydb.IndexMetaData;
import edu.buffalo.cse562.utils.TableUtils;

public class BerekelyDBDataSource implements DataSource,DataSourceReader{

	private BlockingQueue<LeafValue[]> buffer = new ArrayBlockingQueue<>(QUEUE_SIZE);
	private TupleBinding<LeafValue[]> binding;
	private ExpressionTriplets primaryIndexExp;
	private ExpressionTriplets secondaryIndexExp;
	private IndexMetaData indexData;
	private String tableName;
	private Thread producerThread;
	private volatile boolean start;
	private static final int QUEUE_SIZE=2000;
	
	public void setExpression(Expression expression) {
		try {
			List<ExpressionTriplets> triplets  = TableUtils.getIndexableColumns(expression);
			for(ExpressionTriplets temp : triplets){
				if(indexData.getPrimaryIndexName().equals(temp.getColumn().getWholeColumnName())){
					primaryIndexExp = temp;
				}
				else{
					if(temp.getOperator() instanceof EqualsTo){
						if(indexData.getSecondaryIndexes().containsKey(temp.getColumn().getWholeColumnName()))
						secondaryIndexExp=temp;
					}
				}
			}
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public BerekelyDBDataSource(String tableName) {
		this.tableName = tableName;
		indexData = TableUtils.tableIndexMetaData.get(tableName);
		binding=indexData.getBinding();
	}
	
	@Override
	public DataSourceReader getReader() throws IOException {
		close();
		if(primaryIndexExp==null && secondaryIndexExp==null){
			producerThread = new Thread(new Runnable() {
				@Override
				public void run() {
					lookupAll(binding, indexData.getPrimaryDatabase());
				}});
		}
		else if(primaryIndexExp!=null){
			producerThread = new Thread(new Runnable() {
				@Override
				public void run() {
					lookupPrimaryIndex(tableName, primaryIndexExp.getLeafValue(),binding, indexData.getPrimaryDatabase());
				}});
		}else{
			producerThread = new Thread(new Runnable() {
				@Override
				public void run() {
					String secIndexName = tableName+"."+secondaryIndexExp.getColumn().getColumnName();
					SecondaryDatabase db = indexData.getSecondaryIndexes().get(secIndexName);
					lookupSecondaryIndexes(secIndexName, secondaryIndexExp.getLeafValue(), binding,db);
				}});
		}
		producerThread.setDaemon(true);
		start=false;
		return this;
	}

	@Override
	public DataSourceWriter getWriter() throws IOException {
		return null;
	}

	@Override
	public void clear() {
	}

	@Override
	public LeafValue[] readNextTuple() throws IOException {
		try {
			if(!start){
				start=true;
				producerThread.start();
			}
			LeafValue[] values = buffer.take();
			if(values.length == 1 && values[0]==null){
				producerThread=null;
				start=false;
				return null;
			}
			else
				return values;
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}
		return null;
	}
	
	@Override
	public void close() throws IOException {
		if(producerThread!=null){
//			producerThread.interrupt();
//			System.out.println("Thread interrupted :"+producerThread.getName());
			buffer=new ArrayBlockingQueue<>(QUEUE_SIZE);
		}
	}
	
	public synchronized void lookupAll(TupleBinding<LeafValue[]> binding,Database primaryDatabase){
		DiskOrderedCursor cursor=null;
		try{
		DatabaseEntry pkey = new DatabaseEntry();
		DatabaseEntry tuple = new DatabaseEntry();
		//DiskOrderedCursor cursor = primaryDatabase.openCursor(new DiskOrderedCursorConfig());
		cursor = primaryDatabase.openCursor(new DiskOrderedCursorConfig());
		//cursor.get(null, null, LockMode.READ_COMMITTED);
		while(cursor.getNext(pkey, tuple, LockMode.READ_UNCOMMITTED)== OperationStatus.SUCCESS){
			LeafValue[] results = binding.entryToObject(tuple);
			buffer.put(results);
		}
		buffer.put(new LeafValue[1]);
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}finally{
			if(cursor!=null)
				cursor.close();
		}
	}
	
	
	public synchronized void lookupPrimaryIndex(String primaryIndex,LeafValue leafValue,TupleBinding<LeafValue[]> binding,Database primaryDatabase){
		try{
		DatabaseEntry key = new DatabaseEntry();
		TableUtils.bindLeafValueToKey(leafValue, key);
		DatabaseEntry tuple = new DatabaseEntry();
		primaryDatabase.get(null, key, tuple, LockMode.READ_UNCOMMITTED);
		LeafValue[] results = binding.entryToObject(tuple);
		buffer.put(results);
		buffer.put(new LeafValue[1]);
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}
	}
	
	public synchronized void lookupSecondaryIndexes(String secondaryIndexName,LeafValue value,TupleBinding<LeafValue[]> binding,SecondaryDatabase secondaryDb){
		SecondaryCursor cursor=null;
		try{
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry pkey = new DatabaseEntry();
			TableUtils.bindLeafValueToKey(value, key);
			DatabaseEntry tuple = new DatabaseEntry();
			cursor = secondaryDb.openCursor(null, null);
			OperationStatus returnVal = cursor.getSearchKey(key, pkey,tuple, LockMode.READ_UNCOMMITTED);
			while(returnVal== OperationStatus.SUCCESS){
				LeafValue[] results = binding.entryToObject(tuple);
				buffer.put(results);
				returnVal = cursor.getNextDup(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
			}
			buffer.put(new LeafValue[1]);
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}finally{
			if(cursor!=null)
				cursor.close();
		}
	}
	
//	public synchronized void lookupSecondaryIndexes(String secondaryIndexName,LeafValue value,TupleBinding<LeafValue[]> binding,SecondaryDatabase secondaryDb){
//		DatabaseEntry key = new DatabaseEntry();
//		DatabaseEntry pkey = new DatabaseEntry();
//		TableUtils.bindLeafValueToKey(value, key);
//		DatabaseEntry tuple = new DatabaseEntry();
//		SecondaryCursor cursor = secondaryDb.openSecondaryCursor(null, new CursorConfig());
//		cursor.getCurrent(null, null, LockMode.READ_UNCOMMITTED);
//		OperationStatus returnVal = cursor.getSearchKey(key, pkey,tuple, LockMode.READ_UNCOMMITTED);
//		while(returnVal== OperationStatus.SUCCESS){
//			LeafValue[] results = binding.entryToObject(tuple);
//			buffer.add(results);
//			returnVal = cursor.getNextDup(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
//		}
//		buffer.add(null);
//		cursor.close();
//	}
}
