package edu.buffalo.cse562.queryplan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.ExpressionVisitorImpl;
import edu.buffalo.cse562.utils.TableUtils;

public class QueryOptimizer implements QueryDomain {
	
	Set<String> tableNames = new HashSet<String>();

	private Node iterateNode(Node node,Collection<Expression> extractedExpressionList){
		if(node instanceof ProjectNode){
			((ProjectNode) node).setChildNode(iterateNode(((ProjectNode)node).getChildNode(),extractedExpressionList));
			return node;
		}
		else if(node instanceof ExpressionNode){
			//At this point call the util function to extract disintegrated function
			Node childNode = ((ExpressionNode)node).getChildNode();
			List<Expression> expressionList = TableUtils.getBinaryExpressionList(((ExpressionNode)node).getExpression());
			Set<Expression> localExpressionSet = new HashSet<>(expressionList);
			if(expressionList!=null)
				extractedExpressionList.addAll(expressionList);
			childNode = iterateNode(childNode,extractedExpressionList);
			localExpressionSet.retainAll(extractedExpressionList);
			if(localExpressionSet.isEmpty())
				((ExpressionNode) node).setExpressionDead(true);
			((ExpressionNode) node).setChildNode(childNode);
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

						boolean indexLoopJoinSet = false;
						Expression expTemp = ((AbstractJoinNode) node).getJoinCondition();
						if (expTemp instanceof BinaryExpression && ((BinaryExpression) expTemp).getRightExpression() instanceof Column) {
							Column column = (Column) ((BinaryExpression) expTemp).getRightExpression();
							String primaryIndexName = column.getTable().getName() + "." + column.getColumnName();
							if (TableUtils.tableIndexMetaData.containsKey(column.getTable().getName())) {
								if (TableUtils.tableIndexMetaData.get(column.getTable().getName()).getPrimaryIndexName().equals(primaryIndexName)) {
									IndexedLoopJoinNode indexLoopNode = new IndexedLoopJoinNode();
									indexLoopNode.setRelationNode1(((AbstractJoinNode) node).getRelationNode1());
									indexLoopNode.setRelationNode2(((AbstractJoinNode) node).getRelationNode2());
									indexLoopNode.setParentNode(((AbstractJoinNode) node).getParentNode());
									indexLoopNode.addJoinCondition(((AbstractJoinNode) node).getJoinCondition());
									indexLoopJoinSet = true;
									node = indexLoopNode;
								}
							}
						}
						if (!indexLoopJoinSet) {
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
							hashJoinNode.addJoinCondition(((AbstractJoinNode) node).getJoinCondition());
							node=hashJoinNode;
							}
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
		else if(exp instanceof Parenthesis){
			exp.accept(new ExpressionVisitorImpl(this));
			tableNames.addAll(this.tableNames);
			this.tableNames=new HashSet<String>();
		}
		return tableNames;
	}
	
	public Node optimizeQueryPlan(Node node){
		ArrayList<Expression> extractedExpressionList = new ArrayList<Expression>();
		iterateNode(node, extractedExpressionList);
		//System.out.println(node.toString());
		return node;
	}

	@Override
	public Column resolveColumn(Column column) {
		tableNames.add(column.getTable().getName());
		return column;
	}

	@Override
	public Map<String, String> getColumnTableMap() {
		// TODO Auto-generated method stub
		return null;
	}
}
