package edu.buffalo.cse562;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;

public class FromItemImpl implements FromItemVisitor {

	private Node node;

	@Override
	public void visit(Table table) {
		node = new RelationNode(table.getName(),table.getAlias());
	}

	@Override
	public void visit(SubSelect subselect) {
		// TODO Auto-generated method stub
		SelectVisitorImpl selectVistor=new SelectVisitorImpl();
		subselect.getSelectBody().accept(selectVistor);
		node = selectVistor.getQueryPlanTreeRoot();
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Subjoin not supported");
	}
	
	public Node getFromItemNode(){
		return node;
	}
}
