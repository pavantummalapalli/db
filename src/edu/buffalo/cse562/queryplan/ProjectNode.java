package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ProjectNode implements Node {

	private List <String> columnList;
	private List <Function> functionList;
	private List <Node> nodeList;
	private Node childNode;
	private String preferredAliasName;
	
	public void setPreferredAliasName(String preferredAliasName) {
		this.preferredAliasName = preferredAliasName;
	}
	
	public String getPreferredAliasName() {
		return preferredAliasName;
	}
	
	public ProjectNode() {
				
	}
	
	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

	public List<Function> getFunctionList() {
		return functionList;
	}

	public void setFunctionList(List<Function> functionList) {
		this.functionList = functionList;
	}

	public List<Node> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<Node> nodeList) {
		this.nodeList = nodeList;
	}

	public Node getChildNode() {
		return childNode;
	}
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}	
	
	@Override
	public RelationNode eval() {
		RelationNode relationNode = childNode.eval();
		
		try {
			//FileReader fileReader = new FileReader(TableUtils.getDataDir() + File.separator + tableName + ".dat");
			//R.B, R.A
			//check for column size and function list size
			FileReader fileReader = new FileReader(relationNode.getFilePath());
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String rowVal;
			List <ColumnDefinition> columnDefList = relationNode.getSchema().getColumnDefinitions();
			Map <String, Integer> columnIndexMap = new HashMap <>();
			int cnt = 0;
			for (ColumnDefinition columnDef : columnDefList) {
				columnIndexMap.put(columnDef.getColumnName(), cnt++);
			}
			if (columnList != null && !columnList.isEmpty()) {				
				//TODO later bind System.out with PrintWriter and show output.
				StringBuilder sb = new StringBuilder();
				while((rowVal = bufferedReader.readLine()) != null) {
					if (rowVal.trim().isEmpty()) continue;	
					String[] colVals = rowVal.split("\\|");
					for (String column : columnList) {
						sb.append(columnIndexMap.get(column) + "|");
					}
					if (sb.length() > 0) {
						System.out.println(sb.substring(0, sb.length() - 1).toString());
					}
				}
			} else if (functionList != null && !functionList.isEmpty()){
				
			}	
			bufferedReader.close();
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		relationNode.setAliasName(preferredAliasName);
		if(relationNode.getTableName()==null || relationNode.getTableName().isEmpty())
			relationNode.setTableName(preferredAliasName);
		return relationNode;
	}
}
