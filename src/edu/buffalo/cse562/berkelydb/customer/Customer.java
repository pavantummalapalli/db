package edu.buffalo.cse562.berkelydb.customer;

import net.sf.jsqlparser.expression.LeafValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.berkelydb.SecondaryKeyCreaterImpl;

public class Customer {
	
	private SecondaryKeyCreator phoneKey;
	private SecondaryKeyCreator mktSegment;
	
	public Customer(TupleBinding<LeafValue[]> tupleBinding){
		phoneKey = new SecondaryKeyCreaterImpl(tupleBinding, 4);
//		mktSegment = new 
	}
	
	public void setMktSegment(SecondaryKeyCreator mktSegment) {
		this.mktSegment = mktSegment;
	}
	
	public SecondaryKeyCreator getMktSegment() {
		return mktSegment;
	}
	
	public SecondaryKeyCreator getPhoneKey() {
		return phoneKey;
	}
	
	public void setPhoneKey(SecondaryKeyCreator phoneKey) {
		this.phoneKey = phoneKey;
	}
}
