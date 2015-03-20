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

	private Node iterateNode(Node node,List<Expression> extractedExpressionList){
		if(node instanceof ProjectNode){
			((ProjectNode) node).setChildNode(iterateNode(((ProjectNode)node).getChildNode(),extractedExpressionList));
			return node;
		}
		else if(node instanceof ExpressionNode){
			//At this point call the util function to extract disintegrated function
			Node childNode = ((ExpressionNode)node).getChildNode();
			List<Expression> expressionList = TableUtils.getBinaryExpressionList(((ExpressionNode)node).getExpression());
			if(expressionList!=null)
				extractedExpressionList.addAll(expressionList);
			((ExpressionNode) node).setChildNode(iterateNode(childNode,extractedExpressionList));
			return node;
		}
		else if(node instanceof ExtendedProjectNode){
			((ExtendedProjectNode) node).setChildNode(iterateNode(((ExtendedProjectNode)node).getChildNode(),extractedExpressionList));
			return node;
		}
		else if(node instanceof AbstractJoinNode){
			((AbstractJoinNode) node).setRelationNode1(iterateNode(((AbstractJoinNode)node).getRelationNode1(),extractedExpressionList));
			((AbstractJoinNode) node).setRelationNode2(iterateNode(((AbstractJoinNode)node).getRelationNode2(),extractedExpressionList));
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
					if(((AbstractJoinNode)node).getJoinCondition()!=null){
						if(TableUtils.isSwapOn){
							MergeJoinNode sortMerge = new MergeJoinNode(((AbstractJoinNode)node).getRelationNode1(), ((AbstractJoinNode)node).getRelationNode2(), ((AbstractJoinNode)node).getJoinCondition());
							((AbstractJoinNode)node).getParentNode();
							sortMerge.setTableNames(((AbstractJoinNode)node).getTableNames());
							node=sortMerge;
						}else{
							HashJoinNode hashJoinNode = new HashJoinNode();
							hashJoinNode.setRelationNode1(((AbstractJoinNode)node).getRelationNode1());
							hashJoinNode.setRelationNode2(((AbstractJoinNode)node).getRelationNode2());
							hashJoinNode.setParentNode(((AbstractJoinNode)node).getParentNode());
							hashJoinNode.setTableNames(((AbstractJoinNode)node).getTableNames());
							node=hashJoinNode;
						}
					}
				}
			}
			return node;
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
			return node;
		}
		else if(node instanceof UnionOperatorNode){
			List<Node> nodeList = ((UnionOperatorNode) node).getChildNodes();
			List<Node> newNodeList = new ArrayList<Node>();
			for(Node childNode:nodeList)
				newNodeList.add(iterateNode(childNode,extractedExpressionList));
			((UnionOperatorNode) node).setChildNodes(newNodeList);
			return node;
		}
		else
			throw new RuntimeException("Unidentified Instance type");
	}
	
//	private Node iterateNode(Node node,List<Expression> extractedExpressionList){
//		if(node instanceof ProjectNode){
//			((ProjectNode) node).setChildNode(iterateNode(((ProjectNode)node).getChildNode(),extractedExpressionList));
//			return node;
//		}
//		else if(node instanceof ExpressionNode){
//			//At this point call the util function to extract disintegrated function
//			Node childNode = ((ExpressionNode)node).getChildNode();
//			List<Expression> expressionList = TableUtils.getBinaryExpressionList(((ExpressionNode)node).getExpression());
//			if(expressionList!=null)
//				extractedExpressionList.addAll(expressionList);
//			((ExpressionNode) node).setChildNode(iterateNode(childNode,extractedExpressionList));
//			return node;
//		}
//		else if(node instanceof ExtendedProjectNode){
//			((ExtendedProjectNode) node).setChildNode(iterateNode(((ExtendedProjectNode)node).getChildNode(),extractedExpressionList));
//			return node;
//		}
//		else if(node instanceof AbstractJoinNode){
//			((AbstractJoinNode) node).setRelationNode1(iterateNode(((AbstractJoinNode)node).getRelationNode1(),extractedExpressionList));
//			((AbstractJoinNode) node).setRelationNode2(iterateNode(((AbstractJoinNode)node).getRelationNode2(),extractedExpressionList));
//			if(extractedExpressionList!=null){
//				Iterator<Expression> iterator =  extractedExpressionList.iterator();
//				while(iterator.hasNext()){
//					Expression exp = iterator.next();
//					Set<String> listNames =  getTableName(exp);
//					if(listNames.size()==1)
//						continue;
//					Set<String> joinsTableNames = new HashSet<>(((AbstractJoinNode)node).getTableNames());
//					listNames.removeAll(joinsTableNames);
//					if(listNames.isEmpty()){
//						((AbstractJoinNode)node).addJoinCondition(exp);
//						iterator.remove();
//					}
//				}
//			}
//			return node;
//		}
//		else if(node instanceof RelationNode){
//			return node;
//		}
//		else if(node instanceof UnionOperatorNode){
//			List<Node> nodeList = ((UnionOperatorNode) node).getChildNodes();
//			List<Node> newNodeList = new ArrayList<Node>(); 
//			for(Node childNode:nodeList){
//				newNodeList.add(iterateNode(childNode,extractedExpressionList));
//			}
//			((UnionOperatorNode) node).setChildNodes(newNodeList);
//			return node;
//		}
//		else
//			throw new RuntimeException("Unidentified Instance type");
//	}
	
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
		ArrayList<Expression> extractedExpressionList = new ArrayList<Expression>();
		iterateNode(node, extractedExpressionList);
		System.out.println(node.toString());
		return node;
	}
}
