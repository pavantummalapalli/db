package edu.buffalo.cse562.queryplan;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.CartesianProduct;

public class CartesianOperatorNode extends AbstractJoinNode{
	
	public RelationNode eval(){
		CartesianProduct cartesianProduct = new CartesianProduct(getRelationNode1(), getRelationNode2(), getJoinCondition()); 
		return cartesianProduct.doCartesianProduct();
	}
	@Override
	public CreateTable evalSchema() {
		CartesianProduct cartesianProduct = new CartesianProduct(getRelationNode1(), getRelationNode2(), getJoinCondition());
		return cartesianProduct.evalSchema();
	}
	@Override
	public String getJoinName() {
		return "Cartesian";
	}
}
