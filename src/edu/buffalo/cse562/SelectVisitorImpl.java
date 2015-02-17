package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.queryplan.CartesianOperatorNode;
import edu.buffalo.cse562.queryplan.ExpressionNode;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;
import edu.buffalo.cse562.queryplan.UnionOperatorNode;

public class SelectVisitorImpl implements SelectVisitor {

	private Node node;
	
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
		ExpressionNode expressionNode = new ExpressionNode(arg0.getWhere());
		expressionNode.setChildNode(leftNode);
		
		ProjectNode projectNode = new ProjectNode();
		
		List <Node> nodeList = new ArrayList <>();
		List <SelectItem> selectItem = arg0.getSelectItems();		
		List <String> columnList = new ArrayList <>();
		
		for (SelectItem selItem : selectItem) {						
			ProjectItemImpl prjImp = new ProjectItemImpl(leftNode.eval().getTableName());
			selItem.accept(prjImp);
			Node prjNode = prjImp.getSelectItemNode();
			List <String> tempList = prjImp.getSelectColumnList();
			if (prjNode != null) {
				nodeList.add(prjNode);
			}
			if (tempList != null && tempList.size() > 0) {
				columnList.addAll(tempList);
			}			
		}
		projectNode.setColumnList(columnList);
		projectNode.setChildNode(expressionNode);
		projectNode.setNodeList(nodeList);	
		this.node = projectNode.eval();
	}
	
	private Node buildCartesianOperatorNode(Node node,Node node1){
		CartesianOperatorNode cartesianOperatorNode= new CartesianOperatorNode();
		cartesianOperatorNode.setRelationNode1(node);
		cartesianOperatorNode.setRelationNode2(node1);
		return cartesianOperatorNode;
	}

	@Override
	public void visit(Union arg0) {
		UnionOperatorNode node = new UnionOperatorNode();
		List<Node> nodesList = new ArrayList<>();
		arg0.getPlainSelects();
		for(PlainSelect select: (List<PlainSelect>)arg0.getPlainSelects()){
			SelectVisitorImpl impl = new  SelectVisitorImpl();
			select.accept(impl);
			nodesList.add(impl.getQueryPlanTreeRoot());
		}
		node.setChildNodes(nodesList);
		this.node=node;
	}
}
