package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.queryplan.CartesianOperatorNode;
import edu.buffalo.cse562.queryplan.ExpressionNode;
import edu.buffalo.cse562.queryplan.ExtendedProjectNode;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;
import edu.buffalo.cse562.queryplan.UnionOperatorNode;
import edu.buffalo.cse562.utils.TableUtils;

public class SelectVisitorImpl implements SelectVisitor{

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
				visitor.getTableList().addAll(tempVisitor.getTableList());
				leftNode = buildCartesianOperatorNode(leftNode, rightNode);
			}
		}
		columnTableMap =mapColumnAndTable(visitor.getTableList());
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
		boolean extendedMode = false;
		ExtendedProjectNode epn = new ExtendedProjectNode();
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
			epn.setGroupByList(groupByList);
		}
					
		//STEP 4: SET SELECT PROJECTION
		ProjectNode projectNode = new ProjectNode();
		List <SelectItem> selectItem = arg0.getSelectItems();
		List <String> columnList = new ArrayList <>();
		List <Function> functionList = new ArrayList <>();
		for (SelectItem selItem : selectItem) {
			List <String> tableList = visitor.getTableList();
			ProjectItemImpl prjImp = new ProjectItemImpl(tableList, columnTableMap);
			selItem.accept(prjImp);
			Node prjNode = prjImp.getSelectItemNode();
			List <String> tempList = prjImp.getSelectColumnList();
			if (tempList != null && tempList.size() > 0) {
				columnList.addAll(tempList);
			}
			if (prjImp.getFunctionList() != null && !prjImp.getFunctionList().isEmpty()) {
				functionList.addAll(prjImp.getFunctionList());
			}
		}
		projectNode.setColumnList(columnList);
		//If extended mode is true then query is of type select a,sum(a) from B group by a
		if(extendedMode){
			epn.setFunctionList(functionList);
			epn.setChildNode(node);
			node=epn;
			//STEP 4: SET HAVING CLAUSE
			if(arg0.getHaving()!=null){
				ExpressionNode expressionNode = new ExpressionNode(arg0.getHaving());
				expressionNode.setChildNode(node);
				node=expressionNode;
			}
		}
		//Else query is select sum(a) from B
		else{
			projectNode.setFunctionList(functionList);
			projectNode.setChildNode(node);
			node=projectNode;
		}
		
		//STEP 7: SET ORDER BY
		List<OrderByElement> orderByElements =  (List<OrderByElement>)arg0.getOrderByElements();
		projectNode.setOrderByElements(orderByElements);
		//STEP 6: SET DISTINCT
		projectNode.setDistinctOnElements(arg0.getDistinct());
		//STEP 7: SET LIMIT
		projectNode.setLimit(arg0.getLimit());
		node=projectNode;
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
	
	private Map <String, String> mapColumnAndTable(List <String> tableList) {
		columnTableMap = new HashMap <>();
		for (String table : tableList) {
			List <ColumnDefinition> colDefList = TableUtils.getTableSchemaMap().get(table).getColumnDefinitions();
			for (ColumnDefinition columnDef : colDefList) {
				columnTableMap.put(columnDef.getColumnName(), table);
			}
		}	
		return columnTableMap;
	}
}
