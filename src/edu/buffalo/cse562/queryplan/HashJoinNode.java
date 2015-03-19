package edu.buffalo.cse562.queryplan;

import edu.buffalo.cse562.HashJoin;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class HashJoinNode extends CartesianOperatorNode {

	@Override
	public RelationNode eval() {
		HashJoin hashJoin = new HashJoin(getRelationNode1(), getRelationNode2(), getJoinCondition());
		return hashJoin.doHashJoin();
	}

	@Override
	public CreateTable evalSchema() {
		HashJoin hashJoin = new HashJoin(getRelationNode1(), getRelationNode2(), getJoinCondition());
		return hashJoin.evalSchema();
	}

}
