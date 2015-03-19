package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
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
	private Map<String,String> columnTableMap = new HashMap <>();
	private List<SelectExpressionItem> selectExpressionItems;
	
	public Node getQueryPlanTreeRoot(){
		return node;
	}
	
	public void setSelectExpressionItems(
			List<SelectExpressionItem> selectExpressionItems) {
		this.selectExpressionItems = selectExpressionItems;
	}
	
	public List<SelectExpressionItem> getSelectExpressionItems() {
		return selectExpressionItems;
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
		FromItemImpl visitor = new FromItemImpl(this);
		arg0.getFromItem().accept(visitor);
		Node leftNode = visitor.getFromItemNode();
		//Node leafNode = leftNode;
		removeParentFlag(leftNode);
		List<Join> joins=arg0.getJoins();
		if(joins!=null && joins.size()>0){
			Set<String> tableNames  = new HashSet<String>(visitor.getTableNames());
			for(Join join:joins){
				FromItemImpl tempVisitor = new FromItemImpl(this);
				join.getRightItem().accept(tempVisitor);
				Node rightNode =  tempVisitor.getFromItemNode();
				removeParentFlag(rightNode);
				visitor.getTableList().addAll(tempVisitor.getTableList());
				leftNode = buildCartesianOperatorNode(leftNode, rightNode);
				tableNames.addAll(tempVisitor.getTableNames());
				((CartesianOperatorNode)leftNode).setTableNames(new HashSet<>(tableNames));
			}
		}
		columnTableMap =mapColumnAndTable(visitor.getTableList());
		node=leftNode;
		//left Node is the root Node which stores the entire cartesian joins
		//Now apply select filters using where items
		//No premature optimization will be done at this level
		//STEP 2 : SET WHERE CLAUSE
		if(arg0.getWhere()!=null){
			arg0.getWhere().accept(new ExpressionVisitorImpl(this));
			ExpressionNode expressionNode = new ExpressionNode(arg0.getWhere());
			expressionNode.setChildNode(node);
			//leafNode.setExpressionList(expressionList);
			node=expressionNode;
		}
		
		//STEP 3: SET GROUP BY CLAUSE AND PROCESS AGGREGATES
		ProjectNode projectNode = new ProjectNode();
		List <SelectItem> selectItem = arg0.getSelectItems();
		ProjectItemImpl prjImp = new ProjectItemImpl(this);
		for (SelectItem selItem : selectItem) {
			selItem.accept(prjImp);
		}
		selectExpressionItems = prjImp.getSelectExpressionItemList();
		List groupByColumns = arg0.getGroupByColumnReferences();
		List<String> groupByList = new LinkedList<>();
		boolean extendedMode = false;
		//PROCESS AGGREGATES
		ExtendedProjectNode epn = new ExtendedProjectNode();
		if((groupByColumns!=null && groupByColumns.size()>0) || isAggregateFunctionInSelecItem(prjImp.getSelectExpressionItemList())){
			extendedMode=true;
			if(groupByColumns!=null){
				for(Column column:(List<Column>)groupByColumns){
					String wholeCoumnName;
					if(column.getTable()==null ||column.getTable().getName()==null||column.getTable().getName().isEmpty())
						wholeCoumnName =  TableUtils.resolveColumnTableName(columnTableMap, column);
					else
						wholeCoumnName=column.getWholeColumnName();
					groupByList.add(wholeCoumnName.toUpperCase());
				}
				epn.setGroupByList(groupByList);
			}
		}
		resolveSelectItemExpressionList(prjImp.getSelectExpressionItemList());
		//STEP 4: SET SELECT PROJECTION
		//If extended mode is true then query is of type select a,sum(a) from B group by a
		if(extendedMode){
			List<SelectExpressionItem> items = extractAggregateFunctionsFromSelectExpressionItems(prjImp.getSelectExpressionItemList()); 
			epn.setFunctionList(items);
			epn.setChildNode(node);
			node=epn;
			//STEP 4: SET HAVING CLAUSE
			if(arg0.getHaving()!=null){
				arg0.getHaving().accept(new ExpressionVisitorImpl(this));
				ExpressionNode expressionNode = new ExpressionNode(arg0.getHaving());
				expressionNode.setChildNode(node);
				node=expressionNode;
			}
			//Convert all selectExpressionItem to Column types so that no further evaluation can happen
			List<String> selecItemsInStringForm = TableUtils.convertSelectExpressionItemIntoColumnString(prjImp.getSelectExpressionItemList());
			projectNode.setExpressionList(TableUtils.convertColumnListIntoSelectExpressionItem(selecItemsInStringForm));
			projectNode.setChildNode(node);
		}
		//Else query is select sum(a) from B
		else{
			projectNode.setExpressionList(prjImp.getSelectExpressionItemList());
			projectNode.setChildNode(node);
		}
		//STEP 7: SET ORDER BY
		List<OrderByElement> orderByElements =  (List<OrderByElement>)arg0.getOrderByElements();
		//TODO need to evaluate the schema so that order by elements for columns without schema is also possible
		if(orderByElements!=null && orderByElements.size()>0)
			projectNode.setOrderByElements(resolveOrderByElements(orderByElements));
		//STEP 6: SET DISTINCT
		projectNode.setDistinctOnElements(arg0.getDistinct());
		//STEP 7: SET LIMIT		
		projectNode.setLimit(arg0.getLimit());
		node=projectNode;
		
//		List<Expression> expressionList = TableUtils.getBinaryExpressionList(arg0.getWhere());
	}
	
	private boolean isAggregateFunctionInSelecItem(List<SelectExpressionItem> items) {
		for(SelectExpressionItem item:items){
			if(item.getExpression() instanceof Function){
				Function function = (Function)item.getExpression();
				if(isFunctionAggregate(function)){
					return true;
				}
			}
		}
		return false;
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
		
		for (Table table : tableList) {
			List <ColumnDefinition> colDefList = TableUtils.getTableSchemaMap().get(table.getName().toUpperCase()).getColumnDefinitions();
			for (ColumnDefinition columnDef : colDefList) {
				columnTableMap.put(columnDef.getColumnName().toUpperCase(), table.getAlias().toUpperCase());
			}
		}	
		return columnTableMap;
	}
	
	public boolean isFunctionAggregate(Function function){
		if(!function.getName().equalsIgnoreCase("DATE")){
			return true;
		}
		return false;
	}

	@Override
	public Column resolveColumn(Column column) {
		column.setColumnName(column.getColumnName().toUpperCase());
		if(column.getTable().getAlias()!=null)
			column.getTable().setAlias(column.getTable().getAlias().toUpperCase());
//		String resolvedColumn = columnStr;
		if (column.getTable() == null || column.getTable().getName() == null || column.getTable().getName().isEmpty()) {
			Table table;
			if(column.getTable() !=null)
				table = column.getTable(); 
			else
				table = new Table();
			table.setName(columnTableMap.get(column.getWholeColumnName().toUpperCase()));
			return column;
			//resolvedColumn = columnTableMap.get(columnStr) + DOT_STR + columnStr;
		} else {
			column.getTable().setName(column.getTable().getName().toUpperCase());
		}
		
		return column;
	}
	
	public List<SelectExpressionItem> extractAggregateFunctionsFromSelectExpressionItems(List<SelectExpressionItem> selectExpressionItemsList){
		List<SelectExpressionItem> items = new ArrayList<SelectExpressionItem>();
		for(SelectExpressionItem item : selectExpressionItemsList){
			if(item.getExpression() instanceof Function){
				Function function = (Function)item.getExpression();
				if(isFunctionAggregate(function))
					items.add(item);
			}
		}
		return items;
	}
	
	public List<String> resolveFunctionListToColumnList(List<Function> functionList){
		List<String> functionStringList = new ArrayList<String>();
		Iterator<Function> iterator =  functionList.iterator();
		while(iterator.hasNext())
			functionStringList.add(iterator.next().toString());
		return functionStringList;
	}
	
	public void resolveSelectItemExpressionList(List<SelectExpressionItem> items){
		ExpressionVisitorImpl impl = new ExpressionVisitorImpl(this);
		for(SelectExpressionItem item : items){
			item.getExpression().accept(impl);
			if(item.getAlias()!=null)
				item.setAlias(item.getAlias().toUpperCase());
		}
	}
	
	public List<OrderByElement> resolveOrderByElements(List<OrderByElement> orderByElement){
		ExpressionVisitorImpl resolver = new ExpressionVisitorImpl(this);
		Iterator<OrderByElement> iterator = orderByElement.iterator();
		while(iterator.hasNext()){
			iterator.next().getExpression().accept(resolver);
		}
		return orderByElement;
	}
	
	@Override
	public Map<String, String> getColumnTableMap() {
		return columnTableMap;
	}
}
