package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
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
import edu.buffalo.cse562.queryplan.QueryDomain;
import edu.buffalo.cse562.queryplan.UnionOperatorNode;
import edu.buffalo.cse562.utils.TableUtils;

public class SelectVisitorImpl implements SelectVisitor,QueryDomain{

	private Node node;
	private Map<String,String> columnTableMap;
	
	public Node getQueryPlanTreeRoot(){
		return node;
	}
	
	private void removeParentFlag(Node node){
		if(node instanceof ProjectNode){
			((ProjectNode)node).setParentNode(false);
		}
	}
	
	//This is the heart of the solution. The query plan root node will be extracted from here
	@SuppressWarnings("rawtypes")
	@Override
	public void visit(PlainSelect arg0) {
		//STEP 1 : SET FROM CLAUSE
		FromItemImpl visitor = new FromItemImpl();
		arg0.getFromItem().accept(visitor);
		Node leftNode = visitor.getFromItemNode();
		removeParentFlag(leftNode);
		List<Join> joins=arg0.getJoins();
		if(joins!=null && joins.size()>0){
			for(Join join:joins){
				FromItemImpl tempVisitor = new FromItemImpl();
				join.getRightItem().accept(tempVisitor);
				Node rightNode =  tempVisitor.getFromItemNode();
				removeParentFlag(rightNode);
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
			ExpressionNode expressionNode = new ExpressionNode(new ExpressionResolver(this).resolveExpression(arg0.getWhere()));
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
		ProjectItemImpl prjImp = new ProjectItemImpl(this);
		for (SelectItem selItem : selectItem) {
			selItem.accept(prjImp);
		}
		List <String> columnList = prjImp.getSelectColumnList();
		List <Function> functionList = prjImp.getFunctionList();
		List <Expression> expressionList = prjImp.getExpressionList();
		resolveFunctionList(functionList);
		projectNode.setColumnList(columnList);
		projectNode.setExpressionList(expressionList);
		//If extended mode is true then query is of type select a,sum(a) from B group by a
		if(extendedMode){
			epn.setFunctionList(functionList);
			columnList.addAll(resolveFunctionListToColumnList(functionList));
			epn.setChildNode(node);
			node=epn;
			//STEP 4: SET HAVING CLAUSE
			if(arg0.getHaving()!=null){
				ExpressionNode expressionNode = new ExpressionNode(new ExpressionResolver(this).resolveExpression(arg0.getHaving()));
				expressionNode.setChildNode(node);
				node=expressionNode;
			}
			projectNode.setChildNode(epn);
		}
		//Else query is select sum(a) from B
		else{
			projectNode.setFunctionList(functionList);
			projectNode.setChildNode(node);
		}
		//STEP 7: SET ORDER BY
		List<OrderByElement> orderByElements =  (List<OrderByElement>)arg0.getOrderByElements();
		if(orderByElements!=null && orderByElements.size()>0)
			projectNode.setOrderByElements(resolveOrderByElements(orderByElements));	
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
	
	private Map <String, String> mapColumnAndTable(List <Table> tableList) {
		columnTableMap = new HashMap <>();
		for (Table table : tableList) {
			List <ColumnDefinition> colDefList = TableUtils.getTableSchemaMap().get(table.getName().toUpperCase()).getColumnDefinitions();
			for (ColumnDefinition columnDef : colDefList) {
				columnTableMap.put(columnDef.getColumnName().toUpperCase(), table.getAlias().toUpperCase());
			}
		}	
		return columnTableMap;
	}

	@Override
	public Column resolveColumn(Column column) {
		String columnStr = column.getWholeColumnName().toUpperCase();
//		String resolvedColumn = columnStr;
		if (column.getTable() == null || column.getTable().getName() == null || column.getTable().getName().isEmpty()) {
			Table table;
			if(column.getTable() !=null)
				table = column.getTable(); 
			else
				table = new Table();
			table.setName(columnTableMap.get(columnStr.toUpperCase()));
			return column;
			//resolvedColumn = columnTableMap.get(columnStr) + DOT_STR + columnStr;
		} else {
			column.getTable().setName(column.getTable().getName().toUpperCase());
		}
		return column;
	}
	
	public List<String> resolveFunctionListToColumnList(List<Function> functionList){
		List<String> functionStringList = new ArrayList<String>();
		Iterator<Function> iterator =  functionList.iterator();
		while(iterator.hasNext())
			functionStringList.add(iterator.next().toString());
		return functionStringList;
	}
	
	public List<Function> resolveFunctionList(List<Function> functionList){
		Iterator<Function> iterator =  functionList.iterator();
		while(iterator.hasNext())
			resolveFunction(iterator.next());
		return functionList;
	}
	
	public List<OrderByElement> resolveOrderByElements(List<OrderByElement> orderByElement){
		ExpressionResolver resolver = new ExpressionResolver(this);
		Iterator<OrderByElement> iterator = orderByElement.iterator();
		while(iterator.hasNext()){
			resolver.resolveExpression(iterator.next().getExpression());
		}
		return orderByElement;
	}
	
	public Function resolveFunction(Function function) {
		ExpressionResolver resolver = new ExpressionResolver(this);
		if(function.getParameters()!=null){
		Iterator<Expression> expressionIterator = function.getParameters().getExpressions().iterator();
		while(expressionIterator.hasNext())
			resolver.resolveExpression(expressionIterator.next());
		}
		return function;
	}

	@Override
	public Map<String, String> getColumnTableMap() {
		return columnTableMap;
	}
}
