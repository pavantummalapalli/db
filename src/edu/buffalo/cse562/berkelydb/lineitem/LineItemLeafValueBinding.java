package edu.buffalo.cse562.berkelydb.lineitem;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.buffalo.cse562.utils.TableUtils;

public class LineItemLeafValueBinding extends TupleBinding<LeafValue[]> {

	@Override
	public void objectToEntry(LeafValue[] arg0, TupleOutput to) {
		int i=0;
		 try {
			 to.writeLong(arg0[i++].toLong()); //orderkey
             to.writeLong(arg0[i++].toLong()); //partkey
             to.writeLong(arg0[i++].toLong()); //suppkey
             to.writeLong(arg0[i++].toLong()); //linenumber
             to.writeDouble(arg0[i++].toDouble()); //quantity
             to.writeDouble(arg0[i++].toDouble()); //extendedprice
             to.writeDouble(arg0[i++].toDouble()); //discount
             to.writeDouble(arg0[i++].toDouble()); //tax
			 to.writeString(arg0[i++].toString()); //returnflag
			 to.writeString(arg0[i++].toString()); //linestatus
			 to.writeLong(((DateValue)arg0[i++]).getValue().getTime()); //shipdate
             to.writeLong(((DateValue)arg0[i++]).getValue().getTime()); //commitdate
             to.writeLong(((DateValue)arg0[i++]).getValue().getTime()); //receiptdate
			 to.writeString(arg0[i++].toString()); //shipinstruct
			 to.writeString(arg0[i++].toString()); // shipmode
			to.writeString("''"); // comment
		 } catch (InvalidLeaf e) {
				e.printStackTrace();
		 }
	}

	@Override
	public LeafValue[] entryToObject(TupleInput ti) {
		LeafValue[] results = new LeafValue[16];
		int i=0;
		results[i++]=new LongValue(ti.readLong());
        results[i++]=new LongValue(ti.readLong());
        results[i++]=new LongValue(ti.readLong());
        results[i++]=new LongValue(ti.readLong());
        results[i++]=new DoubleValue(ti.readDouble());
        results[i++]=new DoubleValue(ti.readDouble());
        results[i++]=new DoubleValue(ti.readDouble());
        results[i++]=new DoubleValue(ti.readDouble());
		results[i++]=new StringValue(ti.readString());
		results[i++]=new  StringValue(ti.readString());
        results[i++]=TableUtils.getDateValueFromLongValue(ti.readLong());
        results[i++]=TableUtils.getDateValueFromLongValue(ti.readLong());
        results[i++]=TableUtils.getDateValueFromLongValue(ti.readLong());
		results[i++]=new StringValue(ti.readString()); 
		results[i++]=new StringValue(ti.readString());
		results[i++]=new StringValue(ti.readString());
		return results;
	}
}
