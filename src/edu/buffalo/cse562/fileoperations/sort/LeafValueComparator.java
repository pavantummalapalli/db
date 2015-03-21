package edu.buffalo.cse562.fileoperations.sort;

import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.utils.TableUtils;

public class LeafValueComparator implements Comparator<LeafValue[]> {

	private List<Integer> colIndexList;
	
	public LeafValueComparator(List<Integer> colIndexList) {
		this.colIndexList = colIndexList;
	}
	
	@Override
	public int compare(LeafValue[] leafValue1, LeafValue[] leafValue2) {
		for (int colIndex : colIndexList) {
			int compareResult = TableUtils.compareTwoLeafValues(leafValue1[colIndex], leafValue2[colIndex]);
			if (compareResult == 0) continue;
			return compareResult;
		}
		return 0;
	}	
}
