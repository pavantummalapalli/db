package edu.buffalo.cse562.berkelydb;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.ExpressionTriplets;

public class Range {

	private LeafValue maxValue;
	private LeafValue minValue;
	private boolean maxValueIncluded;
	private boolean minValueIncluded;
	private boolean equals;
	private LeafValue equalValue;
	private ExpressionTriplets expressionTriplets;

	public void setExpressionTriplets(ExpressionTriplets expressionTriplets) {
		this.expressionTriplets = expressionTriplets;
	}

	public ExpressionTriplets getExpressionTriplets() {
		return expressionTriplets;
	}

	public boolean isEquals() {
		return equals;
	}

	public void setEquals(boolean equals) {
		this.equals = equals;
	}

	public LeafValue getEqualValue() {
		return equalValue;
	}

	public void setEqualValue(LeafValue equalValue) {
		this.equalValue = equalValue;
	}

	public LeafValue getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(LeafValue maxValue) {
		this.maxValue = maxValue;
	}

	public LeafValue getMinValue() {
		return minValue;
	}

	public void setMinValue(LeafValue minValue) {
		this.minValue = minValue;
	}

	public boolean isMaxValueIncluded() {
		return maxValueIncluded;
	}

	public void setMaxValueIncluded(boolean maxValueIncluded) {
		this.maxValueIncluded = maxValueIncluded;
	}

	public boolean isMinValueIncluded() {
		return minValueIncluded;
	}

	public void setMinValueIncluded(boolean minValueIncluded) {
		this.minValueIncluded = minValueIncluded;
	}

}
