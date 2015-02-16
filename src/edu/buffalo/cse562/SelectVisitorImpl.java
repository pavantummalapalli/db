package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.queryplan.CartesianOperatorNode;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;

public class SelectVisitorImpl implements SelectVisitor {

	private ProjectNode node;
	
	public Node getQueryPlanTreeRoot(){
		return node;
	}
	
	//This is the heart of the solution. The query plan root node will be extracted from here
	@Override
	public void visit(PlainSelect arg0) {
		FromItemImpl visitor = new FromItemImpl();
		arg0.getFromItem().accept(visitor);
		Node leftNode = visitor.getFromItemNode();
		if(arg0.getJoins()!=null && arg0.getJoins().size()>0){
			List<Join> joins = (List<Join>)arg0.getJoins();
			for(Join join:joins){
				FromItemImpl tempVisitor = new FromItemImpl();
				join.getRightItem().accept(tempVisitor);
				Node rightNode =  tempVisitor.getFromItemNode();
				leftNode = buildCartesianOperatorNode(leftNode, rightNode);
			}
		}
		//left Node is the root Node which stores the entire cartesian joins
		//Now apply select filters using where items
		//No premature optimization will be done at this level
		//TODO where item handling
		//arg0.getWhere().
	}
	
	private Node buildCartesianOperatorNode(Node node,Node node1){
		CartesianOperatorNode cartesianOperatorNode= new CartesianOperatorNode();
		cartesianOperatorNode.setRelationNode1(node);
		cartesianOperatorNode.setRelationNode2(node1);
		return cartesianOperatorNode;
	}

	@Override
	public void visit(Union arg0) {
		
		arg0.getPlainSelects();
		// TODO Auto-generated method stub
	}
}
