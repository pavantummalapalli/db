package edu.buffalo.cse562.fileoperations.sort;

import java.util.Comparator;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.utils.TableUtils;

public class LeafValueComparator implements Comparator<LeafValue[]> {

	private int colIndex;
	
	public LeafValueComparator(int colIndex) {
		this.colIndex = colIndex;
	}
	
	@Override
	public int compare(LeafValue[] leafValue1, LeafValue[] leafValue2) {
		return TableUtils.compareTwoLeafValues(leafValue1[colIndex], leafValue2[colIndex]);
	}	
}
