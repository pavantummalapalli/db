package edu.buffalo.cse562.queryplan;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.DataSourceSqlIterator;
import edu.buffalo.cse562.MergeJoinImpl;
import edu.buffalo.cse562.SqlIterator;
import edu.buffalo.cse562.fileoperations.sort.ExternalSort;
import edu.buffalo.cse562.fileoperations.sort.LeafValueComparator;
import edu.buffalo.cse562.fileoperations.sort.LeafValueConverter;
import edu.buffalo.cse562.fileoperations.sort.LeafValueMerger;
import edu.buffalo.cse562.utils.TableUtils;

public class MergeJoinNode implements Operator {

	private Expression expression;
	private Node node1;
	private Node node2;
	private Node parentNode;
		
	public MergeJoinNode(Node relationNode1, Node relationNode2, Expression expression, Node parentNode) {
		this.node1 = relationNode1;
		this.node2 = relationNode2;
		this.expression = expression;
		this.parentNode = parentNode;
	}	
	public Expression getExpression() {
		return expression;
	}
	public void setExpression(Expression expression) {
		this.expression = expression;
	}
	public Node getNode1() {
		return node1;
	}
	public void setNode1(Node node1) {
		this.node1 = node1;
	}
	public Node getNode2() {
		return node2;
	}
	public void setNode2(Node node2) {
		this.node2 = node2;
	}

	@Override
	public RelationNode eval() {
		
		int[] columnIndex = getExpressionColumnIndex();
		//sorting node1
		File[] sortedBlockFiles1 = getSortedBlockFiles(node1, columnIndex[0]);
		File[] sortedBlockFiles2 = getSortedBlockFiles(node2, columnIndex[1]);
		
		List <ColumnDefinition> columnDefList1 = ((RelationNode)node1).getTable().getColumnDefinitions();
		ExternalSort<LeafValue[]> externalSort1 = new ExternalSort<>(new LeafValueComparator(columnIndex[0]), new LeafValueMerger(), new LeafValueConverter(columnDefList1));
		File finalSortedFiles1 = new File(TableUtils.getTempDataDir() + File.separator + "finalSortedFile1");
		externalSort1.externalSort(sortedBlockFiles1, finalSortedFiles1);
		
		List <ColumnDefinition> columnDefList2 = ((RelationNode)node2).getTable().getColumnDefinitions();
		ExternalSort<LeafValue[]> externalSort2 = new ExternalSort<>(new LeafValueComparator(columnIndex[1]), new LeafValueMerger(), new LeafValueConverter(columnDefList2));
		File finalSortedFiles2 = new File(TableUtils.getTempDataDir() + File.separator + "finalSortedFile2");
		externalSort2.externalSort(sortedBlockFiles2, finalSortedFiles2);
		
		//sorting node2		
		FileDataSource fileDataSource1 = (FileDataSource)((RelationNode)node1).getFile();
		fileDataSource1.setFile(finalSortedFiles1);
		
		FileDataSource fileDataSource2 = (FileDataSource)((RelationNode)node2).getFile();
		fileDataSource2.setFile(finalSortedFiles2);
		
		MergeJoinImpl mergeJoin = new MergeJoinImpl(node1, node2, expression);
		
		return mergeJoin.doMergeJoins();
	}	
	
	//TODO port to common utils later	
	private int[] getExpressionColumnIndex() {
		int[] columnIndex = new int[2];
		Expression leftExpression = ((BinaryExpression)expression).getLeftExpression();
		Expression rightExpression = ((BinaryExpression)expression).getRightExpression();
		CreateTable table1 = ((RelationNode)node1).getTable();
		CreateTable table2 = ((RelationNode)node2).getTable();
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
		columnIndex[0] = indexCol1;
		columnIndex[1] = indexCol2;
		return columnIndex;
	}
	
	/**
	 * 
	 * @param node
	 * @param colIndex is the index of the column of the table on which basis we have to sort.
	 * @return
	 */
	private File[] getSortedBlockFiles(Node node, int colIndex) {
		RelationNode relationNode = (RelationNode)node;
		CreateTable table = relationNode.getTable();
		
		FileDataSource fileDataSource = (FileDataSource)relationNode.getFile();
		File file = fileDataSource.getFile();
					
		/**
		 * expression list and group by list is null.
		 */
		SqlIterator sqlIterator = new DataSourceSqlIterator(table, null, fileDataSource, null, relationNode.getExpression());
							
		List <LeafValue[]> leafValueList = new ArrayList <>();
		List <File> sortedFileBlocks = new ArrayList <>();
		
		int fileCount = 1;
		long fileSizeInKB = (file.length() / 1024);
		long memoryAvailableInKB = ExternalSort.getAvailableMemoryInKB();
		long blocksCount = (fileSizeInKB * 2) / memoryAvailableInKB;
		long eachBlockSizeInKB = fileSizeInKB / blocksCount;
		
		LeafValue[] leafValue = null;
		
		while ((leafValue = sqlIterator.next()) != null) {
			leafValueList.add(leafValue);
						
			long currentMemoryAvailable = ExternalSort.getAvailableMemoryInKB();
			if (currentMemoryAvailable - memoryAvailableInKB > eachBlockSizeInKB) {
				File sortedFile = sortAndFlushInFile(leafValueList, colIndex, fileCount++);
				sortedFileBlocks.add(sortedFile);
				leafValueList.clear();
				System.gc();
				memoryAvailableInKB = ExternalSort.getAvailableMemoryInKB();
			}			
		}
		
		if (leafValueList.size() > 0) {
			File sortedFile = sortAndFlushInFile(leafValueList, colIndex, fileCount++);
			sortedFileBlocks.add(sortedFile);
		}
				
		return sortedFileBlocks.toArray(new File[sortedFileBlocks.size()]);
	}
	
	private File sortAndFlushInFile(List <LeafValue[]>leafValueList, int colIndex, int fileCount) {
		File fileTemp = new File(TableUtils.getTempDataDir() + File.separator + "temp" + fileCount);
		if (fileTemp.exists()) 
			fileTemp.delete();
		Collections.sort(leafValueList, new LeafValueComparator(colIndex));
		
		try {
			PrintWriter pw = new PrintWriter(fileTemp);
			for (LeafValue[] leafValue : leafValueList) {
				for (int i = 0; i < leafValue.length - 1; i++) {
					pw.print(leafValue[i] + "|");
				}
				pw.println(leafValue[leafValue.length - 1]);
			}
			pw.close();
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Got FileNotFound Exception " );
		}	
		return fileTemp;
	}
	
	@Override
	public CreateTable evalSchema() {
		return null;
	}
	@Override
	public Node getParentNode() {
		return parentNode;
	}
	@Override
	public void setParentNode(Node parentNode) {
		this.parentNode = parentNode;
	}
	@Override
	public Expression getJoinCondition() {
		return expression;
	}	
	@Override
	public void addJoinCondition(Expression exp) {
		if(expression!=null){
			Expression exp1 = new AndExpression(expression, exp);
			expression=exp1;
		}
		else
			expression=exp;
	}
}