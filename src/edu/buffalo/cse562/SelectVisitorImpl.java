package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
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
import edu.buffalo.cse562.utils.TableUtils;

public class SelectVisitorImpl implements SelectVisitor {

	private Node node;
	private Map<String,String> columnTableMap;
	
	public Node getQueryPlanTreeRoot(){
		return node;
	}
	
	//This is the heart of the solution. The query plan root node will be extracted from here
	@SuppressWarnings("rawtypes")
	@Override
	public void visit(PlainSelect arg0) {
		//STEP 1 : SET FROM CLAUSE
		FromItemImpl visitor = new FromItemImpl();
		arg0.getFromItem().accept(visitor);
		Node leftNode = visitor.getFromItemNode();
		List<Join> joins=arg0.getJoins();
		if(joins!=null && joins.size()>0){
			for(Join join:joins){
				FromItemImpl tempVisitor = new FromItemImpl();
				join.getRightItem().accept(tempVisitor);
				Node rightNode =  tempVisitor.getFromItemNode();
				leftNode = buildCartesianOperatorNode(leftNode, rightNode);
			}
		}
		node=leftNode;
		//left Node is the root Node which stores the entire cartesian joins
		//Now apply select filters using where items
		//No premature optimization will be done at this level
		//STEP 2 : SET WHERE CLAUSE
		if(arg0.getWhere()!=null){
			ExpressionNode expressionNode = new ExpressionNode(arg0.getWhere());
			expressionNode.setChildNode(node);
			node=expressionNode;
		}
		//STEP 3: SET GROUP BY CLAUSE AND PROCESS AGGREGATES
		List groupByColumns = arg0.getGroupByColumnReferences();
		List<String> groupByList = new LinkedList<>();
		boolean extendedMode=false;
		if(groupByColumns!=null && groupByColumns.size()>0){
			extendedMode=true;
			for(Column column:(List<Column>)groupByColumns){
				String wholeCoumnName;
				if(column.getTable()==null ||column.getTable().getName()==null||column.getTable().getName().isEmpty())
					wholeCoumnName =  TableUtils.resolveColumnTableName(columnTableMap, column);
				else
					wholeCoumnName=column.getWholeColumnName();
				groupByList.add(wholeCoumnName);
			}
		}
		//STEP 4: SET HAVING CLAUSE
		if(arg0.getHaving()!=null){
			
		}
		
		//STEP 5: SET SELECT PROJECTION
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
		projectNode.setChildNode(node);
		projectNode.setNodeList(nodeList);	
		node=projectNode;
		//STEP 7: SET ORDER BY
		
		//STEP 6: SET DISTINCT
		
		//STEP 7: SET LIMIT
		
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
