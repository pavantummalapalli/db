package edu.buffalo.cse562.berkelydb.lineitem;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.buffalo.cse562.utils.TableUtils;

public class LineItemLeafValueBinding extends TupleBinding<LeafValue[]> {

	private static HashMap<Byte, StringValue> shipInstruct = new HashMap<Byte, StringValue>() {
		{
			put((byte) 1, new StringValue("'DELIVER IN PERSON'"));
			put((byte) 2, new StringValue("'TAKE BACK RETURN'"));
			put((byte) 3, new StringValue("'NONE'"));
			put((byte) 4, new StringValue("'COLLECT COD'"));
		}
	};

	private static HashMap<String, Byte> compressedShipInstruct = new HashMap<String, Byte>() {
		{
			put("'DELIVER IN PERSON'", (byte) 1);
			put("'TAKE BACK RETURN'", (byte) 2);
			put("'NONE'", (byte) 3);
			put("'COLLECT COD'", (byte) 4);
		}
	};

	private static HashMap<Byte, StringValue> shipMode = new HashMap<Byte, StringValue>() {
		{
			put((byte) 1, new StringValue("'TRUCK'"));
			put((byte) 2, new StringValue("'MAIL'"));
			put((byte) 3, new StringValue("'REG AIR'"));
			put((byte) 4, new StringValue("'AIR'"));
			put((byte) 5, new StringValue("'FOB'"));
			put((byte) 6, new StringValue("'RAIL'"));
			put((byte) 7, new StringValue("'SHIP'"));

		}
	};

	private static HashMap<String, Byte> compressedShipMode = new HashMap<String, Byte>() {
		{
			put("'TRUCK'", (byte) 1);
			put("'MAIL'", (byte) 2);
			put("'REG AIR'", (byte) 3);
			put("'AIR'", (byte) 4);
			put("'FOB'", (byte) 5);
			put("'RAIL'", (byte) 6);
			put("'SHIP'", (byte) 7);

		}
	};

	@Override
	public void objectToEntry(LeafValue[] arg0, TupleOutput to) {
		int i = -1;
		 try {
			to.writeInt((int) arg0[++i].toLong()); // orderkey
			to.writeInt((int) arg0[++i].toLong()); // partkey
			to.writeInt((int) arg0[++i].toLong()); // suppkey
			to.writeInt((int) arg0[++i].toLong()); // linenumber
			to.writeString(arg0[++i].toString()); // quantity
			to.writeString(arg0[++i].toString()); // extendedprice
			// ++i;
			// ++i;
			to.writeString(arg0[++i].toString()); // discount
			to.writeString(arg0[++i].toString()); // tax
			to.writeChar(((StringValue) arg0[++i]).getValue().charAt(0)); // returnflag
			to.writeChar(((StringValue) arg0[++i]).getValue().charAt(0)); // linestatus
			// to.writeLong(((DateValue)arg0[++i]).getValue().getTime());
			// //shipdate
			// to.writeLong(((DateValue)arg0[++i]).getValue().getTime());
			// //commitdate
			// to.writeLong(((DateValue)arg0[++i]).getValue().getTime());
			// //receiptdate
			to.writeString(arg0[++i].toString()); // shipdate
			to.writeString(arg0[++i].toString());// commitdate
			to.writeString(arg0[++i].toString());// receiptdate
			to.writeByte(compressedShipInstruct.get(arg0[++i].toString())); // shipinstruct
			// to.writeChars( paddata(25, (StringValue) arg0[++i])); //
			// shipinstruct
			// to.writeChars(paddata(10, (StringValue) arg0[++i])); // shipmode
			to.writeByte(compressedShipMode.get(arg0[++i].toString()));// shipmode
			to.writeChars(""); // comment
		 } catch (InvalidLeaf e) {
				e.printStackTrace();
		 }
	}

	private char[] paddata(int length, StringValue arg0) {
		String tempStr = ((StringValue) arg0).getValue();
		char[] temp = Arrays.copyOf(((StringValue) arg0).getValue().toCharArray(), length);
		Arrays.fill(temp, tempStr.length() - 1, length, ' ');
		return temp;
	}

	@Override
	public LeafValue[] entryToObject(TupleInput ti) {
		LeafValue[] results = new LeafValue[16];
		HashSet<Integer> columns = TableUtils.existinColumnsForTable.get("LINEITEM");
		if (columns == null) {
			columns = new HashSet<>();
			Map<String, Integer> columnMapping = TableUtils.physicalColumnMapping.get("LINEITEM");
			List<ColumnDefinition> colDefs = TableUtils.getTableSchemaMap().get("LINEITEM").getColumnDefinitions();
			for (ColumnDefinition temp : colDefs) {
				columns.add(columnMapping.get(temp.getColumnName()));
			}
			TableUtils.existinColumnsForTable.put("LINEITEM", columns);
		}
		int i = -1;
		int temp = ti.readInt();
		results[++i] = new LongValue(temp);
		temp = ti.readInt();
		results[++i] = new LongValue(temp);
		temp = ti.readInt();
		results[++i] = new LongValue(temp);
		temp = ti.readInt();
		results[++i] = new LongValue(temp);
		results[++i] = new DoubleValue(ti.readString());
		results[++i] = new DoubleValue(ti.readString());
		results[++i] = new DoubleValue(ti.readString());
		results[++i] = new DoubleValue(ti.readString());
		String temp1 = String.valueOf(ti.readChar());
		results[++i] = new StringValue("'" + temp1 + "'");
		temp1 = String.valueOf(ti.readChar());
		results[++i] = new StringValue("'" + temp1 + "'");
		temp1 = ti.readString();
		results[++i] = TableUtils.getPooledDateValue("'" + temp1 + "'");
		temp1 = ti.readString();
		results[++i] = TableUtils.getPooledDateValue("'" + temp1 + "'");
		// results[++i]=TableUtils.getDateValueFromLongValue(ti.readLong()):
		// null;
		temp1 = ti.readString();
		results[++i] = TableUtils.getPooledDateValue("'" + temp1 + "'");
		// results[++i] = new StringValue("'" + ti.readChars(25).trim() + "'");
		// results[++i] = new StringValue("'" + ti.readChars(10).trim() + "'");
		byte tempByte = ti.readByte();
		results[++i] = shipInstruct.get(tempByte);
		tempByte = ti.readByte();
		results[++i] = shipMode.get(tempByte);
		results[++i] = new StringValue("''");
		return results;
	}
}
