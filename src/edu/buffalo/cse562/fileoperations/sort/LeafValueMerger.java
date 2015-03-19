package edu.buffalo.cse562.fileoperations.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;

public class LeafValueMerger implements Merger<LeafValue[]> {

	@Override
	public List<LeafValue[]> mergeData(LeafValue[] object1, LeafValue[] object2) {
		// TODO Auto-generated method stub
		return new ArrayList<LeafValue[]>(Arrays.asList(object1, object2));
	}

	@Override
	public LeafValue[] writeMergedData(LeafValue[] object1) {
		return object1;
	}
}
