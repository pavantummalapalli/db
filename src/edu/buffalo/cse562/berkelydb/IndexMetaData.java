package edu.buffalo.cse562.berkelydb;

import java.util.Map;

import net.sf.jsqlparser.expression.LeafValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class IndexMetaData {

	private String primaryIndexName;
	private Map<String,SecondaryDatabase> secondaryIndexes;
	private TupleBinding<LeafValue[]> binding;
	
	public void setBinding(TupleBinding<LeafValue[]> binding) {
		this.binding = binding;
	}
	
	public TupleBinding<LeafValue[]> getBinding() {
		return binding;
	}
	
	public String getPrimaryIndexName() {
		return primaryIndexName;
	}
	
	public void setPrimaryIndexName(String primaryIndexName) {
		this.primaryIndexName = primaryIndexName;
	}
	
	public Map<String, SecondaryDatabase> getSecondaryIndexes() {
		return secondaryIndexes;
	}
	
	public void setSecondaryIndexes(
			Map<String, SecondaryDatabase> secondaryIndex) {
		this.secondaryIndexes = secondaryIndex;
	}
}
