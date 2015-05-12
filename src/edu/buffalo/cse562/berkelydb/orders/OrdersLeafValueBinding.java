package edu.buffalo.cse562.berkelydb.orders;

import java.util.HashMap;

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

public class OrdersLeafValueBinding extends TupleBinding<LeafValue[]> {

	private static HashMap<Byte, StringValue> orderPriority = new HashMap<Byte, StringValue>() {
		{
			put((byte) 1, new StringValue("'1-URGENT'"));
			put((byte) 2, new StringValue("'2-HIGH'"));
			put((byte) 3, new StringValue("'3-MEDIUM'"));
			put((byte) 4, new StringValue("'4-NOT SPECIFIED'"));
			put((byte) 5, new StringValue("'5-LOW'"));
		}
	};

	private static HashMap<String, Byte> compressedOrderPriority = new HashMap<String, Byte>() {
		{
			put("'1-URGENT'", (byte) 1);
			put("'2-HIGH'", (byte) 2);
			put("'3-MEDIUM'", (byte) 3);
			put("'4-NOT SPECIFIED'", (byte) 4);
			put("'5-LOW'", (byte) 5);
		}
	};

	@Override
	public void objectToEntry(LeafValue[] arg0, TupleOutput to) {
		int i=0;
		 try {
			to.writeInt((int) arg0[i++].toLong()); // orderkey
			to.writeInt((int) arg0[i++].toLong()); // custkey
			to.writeChar(((StringValue) arg0[i++]).getValue().charAt(0)); // orderstatus
			to.writeString(arg0[i++].toString()); // totalprice
			to.writeLong(((DateValue) arg0[i++]).getValue().getTime()); // orderdate
			to.writeByte(compressedOrderPriority.get(arg0[i++].toString())); // orderpriority
			to.writeString(((StringValue) arg0[i++]).getValue()); // clerk
			to.writeInt((int) arg0[i++].toLong()); // shippriority
			to.writeString(arg0[i++].toString()); // comment
		 } catch (InvalidLeaf e) {
				e.printStackTrace();
		 }
	}

	@Override
	public LeafValue[] entryToObject(TupleInput ti) {
		LeafValue[] results = new LeafValue[9];
		int i=0;
		results[i++] = new LongValue(ti.readInt());
		results[i++] = new LongValue(ti.readInt());
		results[i++] = new StringValue("'" + String.valueOf(ti.readChar()) + "'");
		results[i++] = new DoubleValue(ti.readString());
		results[i++] = TableUtils.getDateValueFromLongValue(ti.readLong());
		results[i++] = orderPriority.get(ti.readByte());
		results[i++] = new StringValue("'" + ti.readString() + "'");
		results[i++] = new LongValue(ti.readInt());
		results[i++] = new StringValue(ti.readString());
		return results;
	}
}
