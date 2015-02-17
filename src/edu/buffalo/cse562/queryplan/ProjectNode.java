package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectNode implements Node {

	private List <String> columnList;
	private List <Node> nodeList;
	private Node childNode;
	
	public ProjectNode() {
				
	}
	
	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
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
		String tableName = relationNode.getTableName();
		try {
			FileReader fileReader = new FileReader(TableUtils.getDataDir() + File.separator + tableName + ".dat");
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String rowVal;
			while((rowVal = bufferedReader.readLine()) != null) {
				System.out.println(rowVal);
			}
			bufferedReader.close();
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return relationNode;
	}
}
