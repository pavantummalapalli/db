package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.queryplan.Node;

public class SelectQueryEvaluator implements SelectItemVisitor{
	
	private String dataDir;
	private SPUJAEval evaluator = new SPUJAEval();
	Node currentNode;

	public SelectQueryEvaluator(String dataDir) {
		this.dataDir=dataDir;
	}

	@Override
	public void visit(AllColumns allcolumns) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(AllTableColumns alltablecolumns) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(SelectExpressionItem selectexpressionitem) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}
	
	public SPUJAEval getEvaluator(){
		return evaluator;
	}
}
