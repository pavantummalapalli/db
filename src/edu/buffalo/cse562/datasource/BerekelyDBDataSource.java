package edu.buffalo.cse562.datasource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;

import com.sleepycat.bind.tuple.TupleBinding;
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
import edu.buffalo.cse562.berkelydb.Range;
import edu.buffalo.cse562.utils.TableUtils;

public class BerekelyDBDataSource implements DataSource,DataSourceReader{

	private BlockingQueue<LeafValue[]> buffer = new ArrayBlockingQueue<>(QUEUE_SIZE);
	private TupleBinding<LeafValue[]> binding;
	private ExpressionTriplets primaryIndexExp;
	private Map<String, Range> secIndexMap = new HashMap<>();
	private IndexMetaData indexData;
	private String tableName;
	private Thread producerThread;
	private volatile boolean start;
	private static final int QUEUE_SIZE = 2000000;
	
	public void setExpression(Expression expression) {
		secIndexMap = new HashMap<>();
		try {
			List<ExpressionTriplets> triplets  = TableUtils.getIndexableColumns(expression);
			for(ExpressionTriplets temp : triplets){
				if(indexData.getPrimaryIndexName().equals(temp.getColumn().getWholeColumnName())){
					primaryIndexExp = temp;
				}
				else{
					String columnName = temp.getColumn().getWholeColumnName();
					if (indexData.getSecondaryIndexes() != null && indexData.getSecondaryIndexes().containsKey(columnName)) {
						Range range = secIndexMap.get(columnName);
						if (range == null) {
							range = new Range();
							range.setExpressionTriplets(temp);
						}
						if (temp.getOperator() instanceof EqualsTo) {
							range.setEquals(true);
							range.setEqualValue(temp.getLeafValue());
						} else if (temp.getOperator() instanceof MinorThan) {
							range.setMaxValue(temp.getLeafValue());
						} else if (temp.getOperator() instanceof MinorThanEquals) {
							range.setMaxValue(temp.getLeafValue());
							range.setMaxValueIncluded(true);
						} else if (temp.getOperator() instanceof GreaterThanEquals) {
							range.setMinValue(temp.getLeafValue());
							range.setMinValueIncluded(true);
						} else if (temp.getOperator() instanceof GreaterThan) {
							range.setMinValue(temp.getLeafValue());
						} else {
							continue;
						}
						secIndexMap.put(columnName, range);
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
	
	private Range evaluateBestAvailableSecondaryIndex() {
		Iterator<String> iterator = secIndexMap.keySet().iterator();
		Set<Range> equalitySet = new HashSet<>();
		Set<Range> rangeSet = new HashSet<>();
		Set<Range> bothRangeSet = new HashSet<>();
		while (iterator.hasNext()) {
			String temp = iterator.next();
			Range range = secIndexMap.get(temp);
			if (range.isMaxValueIncluded() && range.isMinValueIncluded())
				bothRangeSet.add(range);
			else if (range.isEquals())
				equalitySet.add(range);
			else
				rangeSet.add(range);
		}
		if (equalitySet.size() > 0)
			return equalitySet.iterator().next();
		if (bothRangeSet.size() > 0)
			return bothRangeSet.iterator().next();
		if (rangeSet.size() > 0)
			return rangeSet.iterator().next();
		return null;
	}

	@Override
	public DataSourceReader getReader() throws IOException {
		close();
		if (primaryIndexExp == null && secIndexMap.isEmpty()) {
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
					Range range = evaluateBestAvailableSecondaryIndex();
					ExpressionTriplets secondaryIndexExp = range.getExpressionTriplets();
					String secIndexName = tableName+"."+secondaryIndexExp.getColumn().getColumnName();
					SecondaryDatabase db = indexData.getSecondaryIndexes().get(secIndexName);
					if (!range.isEquals())
						rangeLookupSecondaryIndex(secIndexName,
								range.getMinValue(),
								range.isMinValueIncluded(),
								range.isMaxValueIncluded(),
								range.getMaxValue(), binding, db);
					else
						lookupSecondaryIndexes(secIndexName,
								secondaryIndexExp.getLeafValue(), binding, db);

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
	
	private synchronized void lookupAll(TupleBinding<LeafValue[]> binding, Database primaryDatabase) {
		System.out.println("Started linear scan for table :" + tableName);
		long startTime = System.currentTimeMillis();
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
			System.out.println("End:" + (System.currentTimeMillis() - startTime));
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}finally{
			if(cursor!=null)
				cursor.close();
		}
	}
	
	private synchronized void rangeLookupSecondaryIndex(String secondaryIndexName, LeafValue minValue, boolean minIncluded, boolean maxIncuded, LeafValue maxValue, TupleBinding<LeafValue[]> binding,
			SecondaryDatabase secondaryDb) {
		System.out.println("Started range scan for table :" + tableName);
		long startTime = System.currentTimeMillis();
		SecondaryCursor cursor=null;
		try{
			DatabaseEntry key = new DatabaseEntry();
			if(minValue==null && maxValue==null){
				return ;
			}
			if(minValue==null){
				TableUtils.bindLeafValueToKey(maxValue, key);
				DatabaseEntry tuple = new DatabaseEntry();
				cursor = secondaryDb.openCursor(null, null);
				OperationStatus returnVal = cursor.getSearchKeyRange(key, tuple, LockMode.READ_UNCOMMITTED);
				if(returnVal!=OperationStatus.SUCCESS){
					return;
				}
				if(maxIncuded){
					do {
						LeafValue unwrappedValue = TableUtils.unbindLeafValueToKey(maxValue, key);
						if (TableUtils.compareTwoLeafValues(unwrappedValue, maxValue) > 0)
							break;
						LeafValue[] results = binding.entryToObject(tuple);
						buffer.put(results);
					} while (cursor.getNext(key, tuple, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
					cursor.getSearchKey(key, tuple, LockMode.READ_UNCOMMITTED);
				}
				while (cursor.getPrev(key, tuple, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
					LeafValue[] results = binding.entryToObject(tuple);
					buffer.put(results);
				}
			}else{
				TableUtils.bindLeafValueToKey(minValue, key);
				DatabaseEntry tuple = new DatabaseEntry();
				cursor = secondaryDb.openCursor(null, null);
				OperationStatus returnVal = cursor.getSearchKeyRange(key, tuple, LockMode.READ_UNCOMMITTED);
				if(returnVal!=OperationStatus.SUCCESS){
					return;
				}
				if (!minIncluded) {
					// Skip to next no duplicate entry
					if (cursor.getNextNoDup(key, tuple, LockMode.READ_UNCOMMITTED) != OperationStatus.SUCCESS) {
						return;
					}
				}
				do {
					if(maxValue!=null){
						LeafValue unwrappedValue = TableUtils.unbindLeafValueToKey(maxValue, key);
						if(maxIncuded){
							if(TableUtils.compareTwoLeafValues(unwrappedValue, maxValue)>0)
								break;
						}else{
							if(TableUtils.compareTwoLeafValues(unwrappedValue, maxValue)>=0)
								break;
						}
					}
					LeafValue[] results = binding.entryToObject(tuple);
					buffer.put(results);
				} while (cursor.getNext(key, tuple, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
			}
			buffer.put(new LeafValue[1]);
			System.out.println("End:" + (System.currentTimeMillis() - startTime));
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}finally{
			if(cursor!=null)
				cursor.close();
		}
	}
	
	public LeafValue[] lookupPrimaryIndex(LeafValue leafValue) {
		// lookupPrimaryIndex(tableName, primaryIndexExp.getLeafValue(),
		// binding, indexData.getPrimaryDatabase());
		// System.out.println("Started primary key scan for table :" +
		// tableName);
		long startTime = System.currentTimeMillis();
		DatabaseEntry key = new DatabaseEntry();
		TableUtils.bindLeafValueToKey(leafValue, key);
		DatabaseEntry tuple = new DatabaseEntry();
		OperationStatus status = indexData.getPrimaryDatabase().get(null, key, tuple, LockMode.READ_UNCOMMITTED);
		if (status == OperationStatus.SUCCESS)
			return binding.entryToObject(tuple);
		// System.out.println("End:" + (System.currentTimeMillis() -
		// startTime));
		return null;
	}

	private synchronized void lookupPrimaryIndex(String primaryIndex, LeafValue leafValue, TupleBinding<LeafValue[]> binding, Database primaryDatabase) {
		System.out.println("Started primary key scan for table :" + tableName);
		long startTime = System.currentTimeMillis();
		try{
		DatabaseEntry key = new DatabaseEntry();
		TableUtils.bindLeafValueToKey(leafValue, key);
		DatabaseEntry tuple = new DatabaseEntry();
		primaryDatabase.get(null, key, tuple, LockMode.READ_UNCOMMITTED);
		LeafValue[] results = binding.entryToObject(tuple);
		buffer.put(results);
		buffer.put(new LeafValue[1]);
			System.out.println("End:" + (System.currentTimeMillis() - startTime));
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}
	}
	
	private synchronized void lookupSecondaryIndexes(String secondaryIndexName, LeafValue value, TupleBinding<LeafValue[]> binding, SecondaryDatabase secondaryDb) {
		System.out.println("Started secondary key scan for table :" + tableName);
		long startTime = System.currentTimeMillis();
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
			System.out.println("End:" + (System.currentTimeMillis() - startTime));
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted");
		}finally{
			if(cursor!=null)
				cursor.close();
		}
	}

	public List<LeafValue[]> lookupSecondaryIndexForLineItem(LeafValue value) {
		List<LeafValue[]> buffer = new ArrayList<LeafValue[]>();
		SecondaryDatabase secondaryDb = indexData.getSecondaryIndexes().get("LINEITEM.ORDERKEY");
		System.out.println("Started secondary key scan for table :" + tableName);
		long startTime = System.currentTimeMillis();
		SecondaryCursor cursor = null;
		try {
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry pkey = new DatabaseEntry();
			TableUtils.bindLeafValueToKey(value, key);
			DatabaseEntry tuple = new DatabaseEntry();
			cursor = secondaryDb.openCursor(null, null);
			OperationStatus returnVal = cursor.getSearchKey(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
			while (returnVal == OperationStatus.SUCCESS) {
				LeafValue[] results = binding.entryToObject(tuple);
				buffer.add(results);
				returnVal = cursor.getNextDup(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
			}
			System.out.println("End:" + (System.currentTimeMillis() - startTime));
			return buffer;
		} finally {
			if (cursor != null)
				cursor.close();
		}
	}

	@Override
	public long getEstimatedDataSourceSize() {
		return 200 * 1024 * 1024;
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
