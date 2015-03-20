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
import static edu.buffalo.cse562.utils.TableUtils.toUnescapedString;

public class MergeJoinNode extends AbstractJoinNode {

	private Expression exp;
	private static final double ROW_SIZE_IN_KB= 0.5;
	
	public MergeJoinNode(Node relationNode1, Node relationNode2, Expression expression) {
		setRelationNode1(relationNode1);
		setRelationNode2(relationNode2);
		addJoinCondition(expression);
		this.exp = expression;
	}	

	@Override
	public RelationNode eval() {
		//sorting node1
		RelationNode relationNode1 = getRelationNode1().eval();
		RelationNode relationNode2=  getRelationNode2().eval();
		int[] columnIndex = getExpressionColumnIndex(relationNode1,relationNode2);
		
		File[] sortedBlockFiles1 = getSortedBlockFiles(relationNode1, columnIndex[0]);				
		List <ColumnDefinition> columnDefList1 = (relationNode1.getTable().getColumnDefinitions());
		ExternalSort<LeafValue[]> externalSort1 = new ExternalSort<>(new LeafValueComparator(columnIndex[0]), new LeafValueMerger(), new LeafValueConverter(columnDefList1));
		File finalSortedFiles1 = new File(TableUtils.getTempDataDir() + File.separator + "finalSortedFile1");
		externalSort1.externalSort(sortedBlockFiles1, finalSortedFiles1);
		
		File[] sortedBlockFiles2 = getSortedBlockFiles(relationNode2, columnIndex[1]);
		List <ColumnDefinition> columnDefList2 = (relationNode2.getTable().getColumnDefinitions());
		ExternalSort<LeafValue[]> externalSort2 = new ExternalSort<>(new LeafValueComparator(columnIndex[1]), new LeafValueMerger(), new LeafValueConverter(columnDefList2));

		File finalSortedFiles2 = new File(TableUtils.getTempDataDir() + File.separator + "finalSortedFile2");
		externalSort2.externalSort(sortedBlockFiles2, finalSortedFiles2);
		
		//sorting relationNode2		
		FileDataSource fileDataSource1 = (FileDataSource)relationNode1.getFile();
		fileDataSource1.setFile(finalSortedFiles1);
		
		FileDataSource fileDataSource2 = (FileDataSource)relationNode2.getFile();
		fileDataSource2.setFile(finalSortedFiles2);
		
		MergeJoinImpl mergeJoin = new MergeJoinImpl(relationNode1, relationNode2, getJoinCondition());
		
		return mergeJoin.doMergeJoins();
	}	
	
	//TODO port to common utils later	
	private int[] getExpressionColumnIndex(RelationNode relationNode1,RelationNode relationNode2) {
		int[] columnIndex = new int[2];
		Expression leftExpression = ((BinaryExpression)getJoinCondition()).getLeftExpression();
		Expression rightExpression = ((BinaryExpression)getJoinCondition()).getRightExpression();
		CreateTable table1 = relationNode1.getTable();
		CreateTable table2 = relationNode2.getTable();
		List <ColumnDefinition> colDefList1 = table1.getColumnDefinitions();
		List <ColumnDefinition> colDefList2 = table2.getColumnDefinitions();
		
		int indexCol1 = -1;
		int indexCol2 = -1;
		
		for (int i = 0; i < colDefList1.size(); i++) {
			ColumnDefinition col1 = colDefList1.get(i);
			if (leftExpression instanceof Column && leftExpression.toString().equalsIgnoreCase(col1.getColumnName())
					|| rightExpression instanceof Column && rightExpression.toString().equalsIgnoreCase(col1.getColumnName())) {
				indexCol1 = i;
				break;
			}
		}
		for (int i = 0; i < colDefList2.size(); i++) {
			ColumnDefinition col2 = colDefList2.get(i);
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
	private File[] getSortedBlockFiles(RelationNode relationNode,final int colIndex) {
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
		long blocksCount = (long)((fileSizeInKB * 2) / memoryAvailableInKB);
		long eachBlockSizeInKB = blocksCount == 0 ? fileSizeInKB : fileSizeInKB / blocksCount;
		long numberOfLines =(long)(eachBlockSizeInKB/ROW_SIZE_IN_KB); 
		LeafValue[] leafValue = null;
		while ((leafValue = sqlIterator.next()) != null) {
			leafValueList.add(leafValue);
			if (numberOfLines==leafValueList.size()) {
				File sortedFile = sortAndFlushInFile(leafValueList, colIndex, fileCount++);
				sortedFileBlocks.add(sortedFile);
				leafValueList.clear();
				System.gc();
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
		try {
			Collections.sort(leafValueList, new LeafValueComparator(colIndex));
		
			try {
				PrintWriter pw = new PrintWriter(fileTemp);
				for (LeafValue[] leafValue : leafValueList) {
					for (int i = 0; i < leafValue.length - 1; i++) {
						pw.print(toUnescapedString(leafValue[i]) + "|");
					}
					pw.println(toUnescapedString(leafValue[leafValue.length - 1]));
				}
				pw.close();
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Got FileNotFound Exception " );
			}
		} catch (Exception e) {
			System.out.println("Got exception");
		}	
		return fileTemp;
	}
	
	@Override
	public CreateTable evalSchema() {
		return null;
	}

	@Override
	public String getJoinName() {
		return "Merge Join";
	}
}