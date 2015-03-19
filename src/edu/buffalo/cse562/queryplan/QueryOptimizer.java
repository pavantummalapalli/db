package edu.buffalo.cse562.queryplan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.utils.TableUtils;

public class QueryOptimizer {

	private void iterateNode(Node node,List<Expression> extractedExpressionList){
		if(node instanceof ProjectNode){
			iterateNode(((ProjectNode)node).getChildNode(),extractedExpressionList);
		}
		else if(node instanceof ExpressionNode){
			//At this point call the util function to extract disintegrated function
			Node childNode = ((ExpressionNode)node).getChildNode();
			List<Expression> expressionList = TableUtils.getBinaryExpressionList(((ExpressionNode)node).getExpression());
			if(expressionList==null)
				return;
			else
				extractedExpressionList.addAll(expressionList);
			iterateNode(childNode,extractedExpressionList);
		}
		else if(node instanceof ExtendedProjectNode){
			iterateNode(((ExtendedProjectNode)node).getChildNode(),extractedExpressionList);
		}
		else if(node instanceof AbstractJoinNode){
			iterateNode(((AbstractJoinNode)node).getRelationNode1(),extractedExpressionList);
			iterateNode(((AbstractJoinNode)node).getRelationNode2(),extractedExpressionList);
			if(extractedExpressionList!=null){
				Iterator<Expression> iterator =  extractedExpressionList.iterator();
				while(iterator.hasNext()){
					Expression exp = iterator.next();
					Set<String> listNames =  getTableName(exp);
					if(listNames.size()==1)
						continue;
					Set<String> joinsTableNames = new HashSet<>(((AbstractJoinNode)node).getTableNames());
					listNames.removeAll(joinsTableNames);
					if(listNames.isEmpty()){
						((AbstractJoinNode)node).addJoinCondition(exp);
						iterator.remove();
					}
				}
			}
		}
		else if(node instanceof RelationNode){
			String alias = ((RelationNode)node).getTableName();
			//String table= ((RelationNode)node).getTableName();
			if(extractedExpressionList!=null){
				Iterator<Expression> iterator =  extractedExpressionList.iterator();
				while(iterator.hasNext()){
					Expression exp = iterator.next();
					Set<String> listNames =  getTableName(exp);
					listNames.remove(alias);
					if(listNames.isEmpty()){
						((RelationNode)node).addExpression(exp);
						iterator.remove();
					}
				}
			}
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
	
	private Set<String> getTableName(Expression exp){
		Set<String> tableNames = new HashSet<>();
		if(exp instanceof BinaryExpression){
			if(((BinaryExpression)exp).getLeftExpression() instanceof Column){
				tableNames.add(((Column)((BinaryExpression)exp).getLeftExpression()).getTable().getName());
			}
			if(((BinaryExpression)exp).getRightExpression() instanceof Column){
				tableNames.add(((Column)((BinaryExpression)exp).getRightExpression()).getTable().getName());
			}
		}
		return tableNames;
	}
	
	public Node optimizeQueryPlan(Node node){
		iterateNode(node, new ArrayList<Expression>());
		return node;
	}
}
