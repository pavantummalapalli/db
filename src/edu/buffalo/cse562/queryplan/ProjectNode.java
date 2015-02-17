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

	private List <SelectItem> selectItems;
	private Node node;
	private Node childNode;

	public ProjectNode(List <SelectItem> selectItems) {
		this.selectItems = selectItems;
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
