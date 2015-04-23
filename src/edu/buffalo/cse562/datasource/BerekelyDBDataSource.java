package edu.buffalo.cse562.datasource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.ExpressionTriplets;
import edu.buffalo.cse562.berkelydb.DatabaseManager;
import edu.buffalo.cse562.berkelydb.IndexMetaData;
import edu.buffalo.cse562.berkelydb.customer.CustomerLeafValueBinding;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class BerekelyDBDataSource implements DataSource,DataSourceReader{

	private BlockingQueue<LeafValue[]> buffer = new ArrayBlockingQueue<>(20);
	private TupleBinding<LeafValue[]> binding;
	private ExpressionTriplets primaryIndexExp;
	private ExpressionTriplets secondaryIndexExp;
	private RelationNode node;
	private IndexMetaData indexData;
	private DatabaseManager manager;
	
	public BerekelyDBDataSource(RelationNode node,DatabaseManager dbManager) {
		this.node=node;
		this.manager=dbManager;
		Expression exp =node.getExpression();
		try {
			List<ExpressionTriplets> triplets  = TableUtils.getIndexableColumns(exp);
			indexData = TableUtils.tableIndexMetaData.get(node.getTableName());
			for(ExpressionTriplets temp : triplets){
				if(indexData.getSecondaryIndexes().containsKey(node.getTableName())){
					primaryIndexExp = temp;
				}
				else{
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
		// TODO Auto-generated method stub
	}

	@Override
	public LeafValue[] readNextTuple() throws IOException {
		if(primaryIndexExp!=null){
			return lookupPrimaryIndex(node.getTableName(), primaryIndexExp.getLeafValue());
		}else{
			String secIndexName = node.getTableName()+"."+secondaryIndexExp.getColumn().getColumnName();
			lookupSecondaryIndexes(node.getTableName(),secIndexName, secondaryIndexExp.getLeafValue(), binding, indexData.getSecondaryIndexes().get(secIndexName));
		}
		return buffer.poll();
	}
	
	@Override
	public void close() throws IOException {
		
	}
	
	public LeafValue[] lookupPrimaryIndex(String primaryIndex,LeafValue leafValue){
		//DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
		Database customer =manager.getPrimaryIndex(primaryIndex);
		DatabaseEntry key = new DatabaseEntry();
		TableUtils.bindLeafValueToKey(leafValue, key);
		DatabaseEntry tuple = new DatabaseEntry();
		customer.get(null, key, tuple, LockMode.READ_UNCOMMITTED);
		CustomerLeafValueBinding binding = new CustomerLeafValueBinding();
		LeafValue[] results = binding.entryToObject(tuple);
		manager.close();
		return results;
	}
	
	public void lookupSecondaryIndexes(String primaryIndexName,String indexName,LeafValue value,TupleBinding<LeafValue[]> binding,SecondaryKeyCreator secondaryKey){
//		long start = System.currentTimeMillis();
		//DatabaseManager manager = new DatabaseManager(System.getProperty("user.dir")+"/db");
		SecondaryDatabase customer =manager.getSecondaryIndex(manager.getPrimaryIndex(primaryIndexName), indexName, secondaryKey);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry pkey = new DatabaseEntry();
		TableUtils.bindLeafValueToKey(value, key);
		DatabaseEntry tuple = new DatabaseEntry();
		SecondaryCursor cursor = customer.openSecondaryCursor(null, new CursorConfig());
		cursor.getCurrent(null, null, LockMode.READ_UNCOMMITTED);
		OperationStatus returnVal = cursor.getSearchKey(key, pkey,tuple, LockMode.READ_UNCOMMITTED);
		while(returnVal== OperationStatus.SUCCESS){
			LeafValue[] results = binding.entryToObject(tuple);
			buffer.add(results);
//			for(int i=0;i<results.length;i++)
//				System.out.print(results[i].toString()+ " ");
//			System.out.println();
			returnVal = cursor.getNextDup(key, pkey, tuple, LockMode.READ_UNCOMMITTED);
		}
		cursor.close();
//		System.out.println("Time Taken"+(System.currentTimeMillis() - start));
		//manager.close();
	}
}
