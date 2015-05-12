package edu.buffalo.cse562.berkelydb.lineitem;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class LineItemPrimaryKeyBinding extends TupleBinding<LeafValue[]> {

	@Override
	public LeafValue[] entryToObject(TupleInput ti) {
		int i=0;
		LeafValue[] results = new LeafValue[2];
		results[i++] = new LongValue(ti.readSortedPackedInt());
		results[i++] = new LongValue(ti.readSortedPackedInt());
        return results;
	}

	@Override
	public void objectToEntry(LeafValue[] arg0, TupleOutput to) {
		try {
			to.writeSortedPackedInt((int) arg0[0].toLong()); // orderkey
			to.writeSortedPackedInt((int) arg0[3].toLong()); // partkey
		 } catch (InvalidLeaf e) {
				e.printStackTrace();
		 }
	}
}
