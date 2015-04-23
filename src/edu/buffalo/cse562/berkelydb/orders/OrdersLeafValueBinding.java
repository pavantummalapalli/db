package edu.buffalo.cse562.berkelydb.orders;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;

public class OrdersLeafValueBinding extends TupleBinding<LeafValue[]> {

	@Override
	public void objectToEntry(LeafValue[] arg0, TupleOutput to) {
		int i=0;
		 try {
			 to.writeLong(arg0[i++].toLong()); //orderkey
             to.writeLong(arg0[i++].toLong()); //custkey
             to.writeString(arg0[i++].toString()); //orderstatus
             to.writeDouble(arg0[i++].toDouble()); //totalprice
             to.writeLong(((DateValue) arg0[i++]).getValue().getTime()); //orderdate
			 to.writeString(arg0[i++].toString()); //orderpriority
			 to.writeString(arg0[i++].toString()); //clerk
			 to.writeLong(arg0[i++].toLong()); //shippriority
			 to.writeString(arg0[i++].toString()); //comment
		 } catch (InvalidLeaf e) {
				e.printStackTrace();
		 }
	}

	@Override
	public LeafValue[] entryToObject(TupleInput ti) {
		LeafValue[] results = new LeafValue[9];
		int i=0;
		results[i++]=new LongValue(ti.readLong());
        results[i++]=new LongValue(ti.readLong());
        results[i++]=new StringValue(ti.readString());
        results[i++]=new DoubleValue(ti.readDouble());
        results[i++]=TableUtils.getDateValueFromLongValue(ti.readLong());
		results[i++]=new StringValue(ti.readString());
		results[i++]=new  StringValue(ti.readString());
        results[i++]=new LongValue(ti.readLong());
		results[i++]=new StringValue(ti.readString());
		return results;
	}
}
