package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

public class ExpressionTriplets {

	
	 private Column column;
	 private BinaryExpression operator;
	 private LeafValue leafValue;

      public ExpressionTriplets(Column column, BinaryExpression operator, LeafValue leafValue) {
          this.column = column;
          this.operator = operator;
          this.leafValue = leafValue;
      }

	public Column getColumn() {
		return column;
	}

	public void setColumn(Column column) {
		this.column = column;
	}

	public BinaryExpression getOperator() {
		return operator;
	}

	public void setOperator(BinaryExpression operator) {
		this.operator = operator;
	}

	public LeafValue getLeafValue() {
		return leafValue;
	}

	public void setLeafValue(LeafValue leafValue) {
		this.leafValue = leafValue;
	}
      
       
      
}
