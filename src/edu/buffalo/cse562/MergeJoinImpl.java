package edu.buffalo.cse562;

import static edu.buffalo.cse562.utils.TableUtils.convertColumnDefinitionIntoSelectExpressionItems;
import static edu.buffalo.cse562.utils.TableUtils.convertSelectExpressionItemIntoExpressions;
import static edu.buffalo.cse562.utils.TableUtils.toUnescapedString;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.queryplan.BufferDataSource;
import edu.buffalo.cse562.queryplan.DataSource;
import edu.buffalo.cse562.queryplan.FileDataSource;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class MergeJoinImpl {
	private Node node1;
	private Node node2;
	//predicate should be equals to for SMJ 
	private Expression expression;
	
	public MergeJoinImpl(Node node1, Node node2, Expression expression) {
		this.node1 = node1;
		this.node2 = node2;
		this.expression = expression;
	}
	
	public RelationNode doMergeJoins() {
		RelationNode relationNode1 = node1.eval();
		RelationNode relationNode2 = node2.eval();
		CreateTable table1 = relationNode1.getTable();
		CreateTable table2 = relationNode2.getTable();
		
		List<SelectExpressionItem> items = convertColumnDefinitionIntoSelectExpressionItems(table1.getColumnDefinitions());
		DataSource dataFile1 = relationNode1.getFile();
		DataSource dataFile2 = relationNode2.getFile();
		
		List<Expression> table1ItemsExpression = convertSelectExpressionItemIntoExpressions(items);		
		List<Expression> table2ItemsExpression = convertSelectExpressionItemIntoExpressions(convertColumnDefinitionIntoSelectExpressionItems(table2.getColumnDefinitions()));		
		String newTableName = getNewTableName(table1, table2);
		
		Expression leftExpression = ((BinaryExpression)expression).getLeftExpression();
		Expression rightExpression = ((BinaryExpression)expression).getRightExpression();
		
		List <Column> colDefList1 = table1.getColumnDefinitions();
		List <Column> colDefList2 = table2.getColumnDefinitions();
		
		int indexCol1 = -1;
		int indexCol2 = -1;
		
		for (int i = 0; i < colDefList1.size(); i++) {
			Column col1 = colDefList1.get(i);
			if (leftExpression instanceof Column && leftExpression.toString().equalsIgnoreCase(col1.getColumnName())
					|| rightExpression instanceof Column && rightExpression.toString().equalsIgnoreCase(col1.getColumnName())) {
				indexCol1 = i;
				break;
			}
		}
		for (int i = 0; i < colDefList2.size(); i++) {
			Column col2 = colDefList2.get(i);
			if (leftExpression instanceof Column && leftExpression.toString().equalsIgnoreCase(col2.getColumnName())
					|| rightExpression instanceof Column && rightExpression.toString().equalsIgnoreCase(col2.getColumnName())) {
				indexCol2 = i;
				break;
			}
		}		
		if (indexCol1 == -1 || indexCol2 == -1) {
			throw new RuntimeException("Both columns should be present in col definations of two relationNodes. One on each.");
		}	
		
		DataSource file = null;
		if(TableUtils.isSwapOn)
			file =new FileDataSource( new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat"));
		else
			file = new BufferDataSource();
		
		SqlIterator sqlIterator1 = new DataSourceSqlIterator(table1,table1ItemsExpression , dataFile1,null, relationNode1.getExpression());
		SqlIterator sqlIterator2 = new DataSourceSqlIterator(table2, table2ItemsExpression, dataFile2,null, relationNode2.getExpression());
		
		try {
			PrintWriter pw = new PrintWriter(file.getWriter());
			LeafValue[] colVals1 = sqlIterator1.next();
			LeafValue[] colVals2 = sqlIterator2.next();
			while (colVals1 != null && colVals2 != null) {

				if (colVals1[indexCol1].toString().equals(colVals2[indexCol2].toString())) {
					String prevFirst = colVals1[indexCol1].toString();
					String prevSecon = colVals2[indexCol2].toString();
					List<LeafValue[]> firstList = new ArrayList<>();
					List<LeafValue[]> seconList = new ArrayList <>();
					
					while (prevFirst.equals(colVals1[indexCol1].toString())) {
						LeafValue[] leafValue1 = new LeafValue[colVals1.length];
						for (int i = 0; i < colVals1.length; i++) {
							leafValue1[i] = colVals1[i];
						}
						firstList.add(leafValue1);
						colVals1 = sqlIterator1.next();
					}
					while (prevSecon.equals(colVals2[indexCol2].toString())) {
						LeafValue[] leafValue2 = new LeafValue[colVals2.length];
						for (int i = 0; i < colVals2.length; i++) {
							leafValue2[i] = colVals2[i];
						}
						seconList.add(leafValue2);
						colVals2 = sqlIterator2.next();
					}	
					for (LeafValue[] col1LeafValue : firstList) {
						for (LeafValue[] col2LeafValue : seconList) {
							int i;
							for(i=0; i<col1LeafValue.length; i++) {
								pw.print(toUnescapedString(col1LeafValue[i]) + "|");
							}
							for(i=1; i<col2LeafValue.length; i++) {
								pw.print(toUnescapedString(col2LeafValue[i-1]) + "|");
							}
							if(col2LeafValue.length > 0)
								pw.println(toUnescapedString(col2LeafValue[i-1]));
						}
					}
					
				} else if (TableUtils.compareTwoLeafValues(colVals1[indexCol1], colVals2[indexCol2]) < 0) {
					colVals1 = sqlIterator1.next();
				} else {
					colVals2 = sqlIterator2.next();
				}
			}
			sqlIterator1.close();
			sqlIterator2.close();
			pw.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		List<ColumnDefinition> newList = new ArrayList<ColumnDefinition>();
		newList.addAll(table1.getColumnDefinitions());
		newList.addAll(table2.getColumnDefinitions());
		CreateTable newTable = new CreateTable();
		newTable.setTable(new Table(null, newTableName));
		newTable.setColumnDefinitions(newList);
				
		RelationNode relationNode = new RelationNode(newTableName, null,file,newTable);
		return relationNode;
	}	
	
	private String getNewTableName(CreateTable table1,CreateTable table2){
		return table1.getTable().getName() + "_MERGE_" + table2.getTable().getName();
	}
}
