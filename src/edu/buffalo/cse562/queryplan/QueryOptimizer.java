package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.utils.TableUtils;

public class QueryOptimizer {

	//TODO Convert to visitor patter latter on
	private void iterateNode(Node node,List<Expression> extractedExpressionList){
		if(node instanceof ProjectNode){
			iterateNode(((ProjectNode)node).getChildNode(),extractedExpressionList);
		}
		else if(node instanceof ExpressionNode){
			//At this point call the util function to extract disintegrated function
			Node childNode = ((ExpressionNode)node).getChildNode();
			iterateNode(childNode,extractedExpressionList);
			if(extractedExpressionList==null){
				extractedExpressionList = new ArrayList<Expression>();
			}
			List<Expression> expressionList = TableUtils.getBinaryExpressionList(((ExpressionNode)node).getExpression());
			if(expressionList==null)
				return;
			else
				extractedExpressionList.addAll(expressionList);
		}
		else if(node instanceof ExtendedProjectNode){
			iterateNode(((ExtendedProjectNode)node).getChildNode(),extractedExpressionList);
		}
		else if(node instanceof CartesianOperatorNode){
			iterateNode(((CartesianOperatorNode)node).getRelationNode1(),extractedExpressionList);
			iterateNode(((CartesianOperatorNode)node).getRelationNode2(),extractedExpressionList);
		}
		else if(node instanceof RelationNode){
			return;
		}
		else if(node instanceof UnionOperatorNode){
			List<Node> nodeList = ((UnionOperatorNode) node).getChildNodes();
			for(Node childNode:nodeList)
				iterateNode(childNode,extractedExpressionList);	
		}
		else
			throw new RuntimeException("Unidentified Instance type");
	}
	
	public Node optimizeQueryPlan(ProjectNode node){
		return node;
	}
}
