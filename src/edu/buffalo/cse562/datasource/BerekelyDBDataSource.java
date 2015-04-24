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
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

import edu.buffalo.cse562.ExpressionTriplets;
import edu.buffalo.cse562.berkelydb.IndexMetaData;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class BerekelyDBDataSource implements DataSource,DataSourceReader{

	private BlockingQueue<LeafValue[]> buffer = new ArrayBlockingQueue<>(20);
	private TupleBinding<LeafValue[]> binding;
	private ExpressionTriplets primaryIndexExp;
	private ExpressionTriplets secondaryIndexExp;
	private RelationNode node;
	private IndexMetaData indexData;
	
	public BerekelyDBDataSource(RelationNode node) {
		this.node=node;
		Expression exp =node.getExpression();
		try {
			List<ExpressionTriplets> triplets  = TableUtils.getIndexableColumns(exp);
			indexData = TableUtils.tableIndexMetaData.get(node.getTableName());
			for(ExpressionTriplets temp : triplets){
				if(indexData.getSecondaryIndexes().containsKey(node.getTableName())){
					primaryIndexExp = temp;
				}
				else{
					if(temp.getOperator() instanceof EqualsTo)
						secondaryIndexExp=temp;
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public DataSourceReader getReader() throws IOException {
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
		if(primaryIndexExp!=null){
			return lookupPrimaryIndex(node.getTableName(), primaryIndexExp.getLeafValue(),binding, indexData.getPrimaryDatabase());
		}else{
			new Thread(new Runnable() {
				@Override
				public void run() {
					String secIndexName = node.getTableName()+"."+secondaryIndexExp.getColumn().getColumnName();
					SecondaryDatabase db = indexData.getSecondaryIndexes().get(secIndexName);
					lookupSecondaryIndexes(secIndexName, secondaryIndexExp.getLeafValue(), binding,db);
				}}).start();
		}
		return buffer.poll();
	}
	
	@Override
	public void close() throws IOException {
	}
	
	public LeafValue[] lookupPrimaryIndex(String primaryIndex,LeafValue leafValue,TupleBinding<LeafValue[]> binding,Database primaryDatabase){
		DatabaseEntry key = new DatabaseEntry();
		TableUtils.bindLeafValueToKey(leafValue, key);
		DatabaseEntry tuple = new DatabaseEntry();
		primaryDatabase.get(null, key, tuple, LockMode.READ_UNCOMMITTED);
		LeafValue[] results = binding.entryToObject(tuple);
		return results;
	}
	
	public synchronized void lookupSecondaryIndexes(String secondaryIndexName,LeafValue value,TupleBinding<LeafValue[]> binding,SecondaryDatabase secondaryDb){
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry pkey = new DatabaseEntry();
		TableUtils.bindLeafValueToKey(value, key);
		DatabaseEntry tuple = new DatabaseEntry();
		SecondaryCursor cursor = secondaryDb.openSecondaryCursor(null, new CursorConfig());
		cursor.getCurrent(null, null, LockMode.READ_UNCOMMITTED);
		OperationStatus returnVal = cursor.getSearchKey(key, pkey,tuple, LockMode.READ_UNCOMMITTED);
		while(returnVal== OperationStatus.SUCCESS){
			LeafValue[] results = binding.entryToObject(tuple);
			buffer.add(results);
			returnVal = cursor.getNextDup(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
		}
		buffer.add(null);
		cursor.close();
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
