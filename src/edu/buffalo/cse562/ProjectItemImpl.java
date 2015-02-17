package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.queryplan.ExpressionNode;
import edu.buffalo.cse562.queryplan.Node;

public class ProjectItemImpl implements SelectItemVisitor {

	private Node node;
		
	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
		System.out.println(allColumns.toString());	
		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		System.out.println(allTableColumns.toString());
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		//TODO
		ExpressionNode expNode = new ExpressionNode(selectExpressionItem.getExpression());
		this.node = expNode;
		
	}

	public Node getSelectItemNode() {
		return node;
	}
}
