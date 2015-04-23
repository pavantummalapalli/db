package edu.buffalo.cse562.berkelydb;

import net.sf.jsqlparser.expression.LeafValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.utils.TableUtils;

public class SecondaryKeyCreaterImpl implements SecondaryKeyCreator {

	private TupleBinding<LeafValue[]> tupleBinding;
	private int indexPos;
	
	public SecondaryKeyCreaterImpl(TupleBinding<LeafValue[]> tupleBinding,int indexPos) {
		this.tupleBinding=tupleBinding;
		this.indexPos = indexPos;
	}
	
	@Override
	public boolean createSecondaryKey(SecondaryDatabase arg0,
			DatabaseEntry key, DatabaseEntry tuple, DatabaseEntry result) {
		if(tuple!=null){
            LeafValue[] data =
                  tupleBinding.entryToObject(tuple);
            LeafValue secondaryIndex = data[indexPos];
            TableUtils.bindLeafValueToKey(secondaryIndex, result);
//			TupleBinding.getPrimitiveBinding(String.class).objectToEntry(((StringValue)secondaryIndex).getValue(), result);
		}
		return true;
	}
}
