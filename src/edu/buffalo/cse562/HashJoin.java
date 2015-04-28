package edu.buffalo.cse562;

import static edu.buffalo.cse562.utils.TableUtils.convertColumnDefinitionIntoSelectExpressionItems;
import static edu.buffalo.cse562.utils.TableUtils.convertSelectExpressionItemIntoExpressions;
import static edu.buffalo.cse562.utils.TableUtils.toUnescapedString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.datasource.BufferDataSource;
import edu.buffalo.cse562.datasource.DataSource;
import edu.buffalo.cse562.datasource.DataSourceWriter;
import edu.buffalo.cse562.datasource.FileDataSource;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class HashJoin {
	private Node node1;
	private Node node2;
	private Expression expression;
	public HashJoin(Node node1, Node node2,
			Expression expression) {
		this.node1 = node1;
		this.node2 = node2;
		this.expression = expression;
	}
	
	public RelationNode doHashJoin() {
		try {
		RelationNode relationNode1 = node1.eval();
		RelationNode relationNode2 = node2.eval();
		CreateTable table1 = relationNode1.getTable();
		CreateTable table2 = relationNode2.getTable();
		List<ColumnDefinition> newList = new ArrayList<ColumnDefinition>();
		newList.addAll(table1.getColumnDefinitions());
		newList.addAll(table2.getColumnDefinitions());
		List<SelectExpressionItem> items = convertColumnDefinitionIntoSelectExpressionItems(table1.getColumnDefinitions());
		DataSource dataFile1 = relationNode1.getFile();
		DataSource dataFile2 = relationNode2.getFile();
		List<Expression> table1ItemsExpression = convertSelectExpressionItemIntoExpressions(items);
		List<Expression> joinExpressions = TableUtils.getIndividualJoinConditions(expression);
		List<int[]> colIndexList = new ArrayList<>();
		for(Expression joinExpression: joinExpressions) {
			if(joinExpression instanceof BinaryExpression) {
				List<ColumnDefinition> cdList = table1.getColumnDefinitions();
				int[] colIndex = {0, 0};
				Expression table2exp = null;
				Expression exprLeft = ((BinaryExpression) joinExpression).getLeftExpression();
				Expression exprRight = ((BinaryExpression) joinExpression).getRightExpression();
				for (ColumnDefinition colDef : cdList) {
					if ((exprLeft instanceof Column &&  colDef.getColumnName().equalsIgnoreCase(exprLeft.toString()))) {
						table2exp = exprRight;
						break;
						}
					else if ((exprRight instanceof Column &&  colDef.getColumnName().equalsIgnoreCase(exprRight.toString()))) {
						table2exp = exprLeft;
						break;
					}
					colIndex[0]++;
				}
				cdList = table2.getColumnDefinitions();
				for (ColumnDefinition colDef : cdList) {
					if ((table2exp instanceof Column &&  colDef.getColumnName().equalsIgnoreCase(table2exp.toString()))) {
						break;
					} 
					colIndex[1]++;
				}
				colIndexList.add(colIndex);
			}
		}
		
		Map<String, List<LeafValue[]>> hashMap = new HashMap<>();
		SqlIterator sqlIterator1 = new DataSourceSqlIterator(table1,table1ItemsExpression , dataFile1.getReader(), null, relationNode1.getExpression());
		String newTableName = getNewTableName(table1, table2);
		LeafValue[] colVals1, colVals2;
		DataSource file =null;
		if(TableUtils.isSwapOn)
			file= new FileDataSource(new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat"),newList);
		else
			file = new BufferDataSource();
		DataSourceWriter fileWriter = file.getWriter();
		String delimiter = "!~";
		while((colVals1 = sqlIterator1.next()) != null) { 
			StringBuilder hashKeyBuilder = new StringBuilder();
			for(int[] colIndex: colIndexList) {
				hashKeyBuilder.append(toUnescapedString( colVals1[colIndex[0]]) + delimiter); 
			}
			String hashKey = hashKeyBuilder.substring(0, hashKeyBuilder.length() - delimiter.length());
			List<LeafValue[]> temp;
			if( (temp = hashMap.get(hashKey))!=null) {
				temp.add(colVals1);
			}
			else {
				List<LeafValue[]> list = new ArrayList<>();
				list.add(colVals1);
				hashMap.put(hashKey, list);
			}
		}
		sqlIterator1.close();
		
			relationNode1.getFile().clear();
			System.gc();
			SqlIterator sqlIterator2 = new DataSourceSqlIterator(table2,
					convertSelectExpressionItemIntoExpressions(TableUtils.convertColumnDefinitionIntoSelectExpressionItems(table2.getColumnDefinitions())),
					dataFile2.getReader(), null, relationNode2.getExpression());
			while((colVals2 = sqlIterator2.next()) != null) {
				StringBuilder hashKeyBuilder = new StringBuilder();
				for(int[] colIndex: colIndexList) {
					hashKeyBuilder.append(toUnescapedString(colVals2[colIndex[1]]) + delimiter); 
				}
				String hashKey = hashKeyBuilder.substring(0, hashKeyBuilder.length() - delimiter.length());
				List<LeafValue[]> leafValues=null;
				if((leafValues=hashMap.get(hashKey))!=null) {
					Iterator<LeafValue[]> it = leafValues.iterator();
					while(it.hasNext()) {
						LeafValue[] firstTuple = it.next();
						LeafValue[] tuple = new LeafValue[firstTuple.length+colVals2.length];
						int z=0;
						for(int j=0; j<firstTuple.length; j++,z++)
							tuple[z]=firstTuple[j];
						for(int j=0; j<colVals2.length; j++,z++)
							tuple[z]=colVals2[j];
						fileWriter.writeNextTuple(tuple);
					}
				}
			}
			fileWriter.close();
			sqlIterator2.close();
			relationNode2.getFile().clear();
			System.gc();
		CreateTable newTable = new CreateTable();
		newTable.setTable(new Table(null, newTableName));
		newTable.setColumnDefinitions(newList);
		//TODO put the table name in a temp hash map
		//TableUtils.getTableSchemaMap().put(newTableName, newTable);
		RelationNode relationNode = new RelationNode(newTableName, null,file,newTable);
		return relationNode;
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String getNewTableName(CreateTable table1,CreateTable table2){
		return table1.getTable().getName() + "x" + table2.getTable().getName();
	}
	
	public CreateTable evalSchema(){
		CreateTable table1 = node1.evalSchema();
		CreateTable table2 = node2.evalSchema();
		CreateTable table = new CreateTable();
		table.setTable(new Table());
		List<ColumnDefinition> list = new LinkedList<>();
		list.addAll(table1.getColumnDefinitions());
		list.addAll(table2.getColumnDefinitions());
		table.setColumnDefinitions(list);
		return table;
	}
}
