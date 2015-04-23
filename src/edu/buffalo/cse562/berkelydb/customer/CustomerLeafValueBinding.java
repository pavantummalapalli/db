package edu.buffalo.cse562.berkelydb.customer;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class CustomerLeafValueBinding extends TupleBinding<LeafValue[]> {

	@Override
	public void objectToEntry(LeafValue[] arg0, TupleOutput to) {
		int i=0;
		 try {
			 to.writeLong(arg0[i++].toLong());//custkey
			 to.writeString(arg0[i++].toString()); //name 
			 to.writeString(arg0[i++].toString()); //address
			 to.writeLong(arg0[i++].toLong()); //nationkey
			 to.writeString(arg0[i++].toString()); //phone
			 to.writeDouble(arg0[i++].toDouble()); //acctbal
			 to.writeString(arg0[i++].toString()); // mktsegment
			 to.writeString(arg0[i++].toString()); //comment
		 } catch (InvalidLeaf e) {
				e.printStackTrace();
		 }
	}

	@Override
	public LeafValue[] entryToObject(TupleInput ti) {
		LeafValue[] results = new LeafValue[8];
		int i=0;
		results[i++]=new LongValue(ti.readLong());
		results[i++]=new StringValue(ti.readString());
		results[i++]=new  StringValue(ti.readString());
		results[i++]=new LongValue(ti.readLong()); 
		results[i++]=new StringValue(ti.readString()); 
		results[i++]=new DoubleValue(ti.readDouble());
		results[i++]=new StringValue(ti.readString());
		results[i++]=new StringValue(ti.readString());
		return results;
	}
}
