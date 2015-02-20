package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class UnionOperatorNode implements Operator {

	private List<Node> childNodes;
	
	public void setChildNodes(List<Node> childNodes) {
		this.childNodes = childNodes;
	}

	public List<Node> getChildNodes() {
		return childNodes;
	}

	@Override
	public RelationNode eval() {

		// TODO Auto-generated method stub
		PrintWriter out = new PrintWriter(System.out);
		try {
			for (Node childNode : childNodes) {
				RelationNode relationNode = childNode.eval();
				FileReader fileReader = new FileReader(relationNode.getFile());
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String rowVal;
								
				while ((rowVal = bufferedReader.readLine()) != null) {
					if (rowVal.trim().isEmpty())
						continue;
					out.println(rowVal);
				}
				bufferedReader.close();
				fileReader.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		out.close();
		//TODO as of now returning nothing. Just printing.
		return null;
	}

	@Override
	public CreateTable evalSchema() {
		// TODO Auto-generated method stub
		return null;
	}
}
