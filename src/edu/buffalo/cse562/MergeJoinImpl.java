package edu.buffalo.cse562;

import static edu.buffalo.cse562.utils.TableUtils.convertColumnDefinitionIntoSelectExpressionItems;
import static edu.buffalo.cse562.utils.TableUtils.convertSelectExpressionItemIntoExpressions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.datasource.DataSource;
import edu.buffalo.cse562.datasource.DataSourceWriter;
import edu.buffalo.cse562.queryplan.BufferDataSource;
import edu.buffalo.cse562.queryplan.FileDataSource;
import edu.buffalo.cse562.queryplan.MergeJoinNode;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class MergeJoinImpl {
	private Node node1;
	private Node node2;
	// predicate should be equals to for SMJ
	private Expression expression;

	public MergeJoinImpl(Node node1, Node node2, Expression expression) {
		this.node1 = node1;
		this.node2 = node2;
		this.expression = expression;
	}

	public RelationNode doMergeJoins() {
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
			List<Expression> table2ItemsExpression = convertSelectExpressionItemIntoExpressions(convertColumnDefinitionIntoSelectExpressionItems(table2
					.getColumnDefinitions()));
			String newTableName = getNewTableName(table1, table2);

			List<Integer>[] columnIndexList = MergeJoinNode.getExpressionColumnIndexList(relationNode1, relationNode2, expression);

			DataSource file = null;
			if (TableUtils.isSwapOn)
				file = new FileDataSource(new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat"), newList);
			else
				file = new BufferDataSource();
			DataSourceWriter fileWriter = file.getWriter();
			SqlIterator sqlIterator1 = new DataSourceSqlIterator(table1, table1ItemsExpression, dataFile1.getReader(), null,
					relationNode1.getExpression());
			SqlIterator sqlIterator2 = new DataSourceSqlIterator(table2, table2ItemsExpression, dataFile2.getReader(), null,
					relationNode2.getExpression());
			LeafValue[] colVals1 = sqlIterator1.next();
			LeafValue[] colVals2 = sqlIterator2.next();
			long startTime = System.currentTimeMillis();
			while (colVals1 != null && colVals2 != null) {
				int compareResults = TableUtils.compareTwoLeafValuesList(colVals1, colVals2, columnIndexList);
				if (compareResults == 0) {
					LeafValue[] prevFirstColVals = colVals1;
					LeafValue[] prevSeconColVals = colVals2;

					List<LeafValue[]> firstList = new ArrayList<>();
					List<LeafValue[]> seconList = new ArrayList<>();

					while (colVals1 != null && compareLeafValueArrayForSameRelation(colVals1, prevFirstColVals, columnIndexList[0])) {
						LeafValue[] leafValue1 = new LeafValue[colVals1.length];
						for (int i = 0; i < colVals1.length; i++) {
							leafValue1[i] = colVals1[i];
						}
						firstList.add(leafValue1);
						colVals1 = sqlIterator1.next();
					}
					while (colVals2 != null && compareLeafValueArrayForSameRelation(colVals2, prevSeconColVals, columnIndexList[1])) {
						LeafValue[] leafValue2 = new LeafValue[colVals2.length];
						for (int i = 0; i < colVals2.length; i++) {
							leafValue2[i] = colVals2[i];
						}
						seconList.add(leafValue2);
						colVals2 = sqlIterator2.next();
					}
					for (LeafValue[] col1LeafValue : firstList) {
						for (LeafValue[] col2LeafValue : seconList) {
							LeafValue[] tuple = new LeafValue[col1LeafValue.length + col2LeafValue.length];
							int z = 0;
							for (int i = 0; i < col1LeafValue.length; i++, z++) {
								tuple[z] = col1LeafValue[i];
							}
							for (int i = 0; i < col2LeafValue.length; i++, z++) {
								tuple[z] = col2LeafValue[i];
							}
							fileWriter.writeNextTuple(tuple);
						}
					}
				} else if (compareResults < 0) {
					colVals1 = sqlIterator1.next();
				} else {
					colVals2 = sqlIterator2.next();
				}
			}
			long endTime = System.currentTimeMillis();
			// System.out.println("Time taken on core merge join " + (endTime - startTime));
			fileWriter.close();
			sqlIterator1.close();
			sqlIterator2.close();
			dataFile1.clear();
			dataFile2.clear();
			CreateTable newTable = new CreateTable();
			newTable.setTable(new Table(null, newTableName));
			newTable.setColumnDefinitions(newList);

			RelationNode relationNode = new RelationNode(newTableName, null, file, newTable);
			return relationNode;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean compareLeafValueArrayForSameRelation(LeafValue[] prevLeafValue, LeafValue[] currLeafValue, List<Integer> columnList) {
		boolean ans = true;
		for (int i = 0; i < columnList.size() && ans; i++) {
			int index = columnList.get(i);
			ans = ans & TableUtils.compareTwoLeafValues(prevLeafValue[index], currLeafValue[index]) == 0;
		}
		return ans;
	}

	private String getNewTableName(CreateTable table1, CreateTable table2) {
		return table1.getTable().getName() + "_MERGE_" + table2.getTable().getName();
	}
}
