package edu.buffalo.cse562.queryplan;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.IndexLoopJoin;

public class IndexedLoopJoinNode extends AbstractJoinNode {

	@Override
	public RelationNode eval() {
		IndexLoopJoin indexJoin = new IndexLoopJoin(getRelationNode1(), getRelationNode2(), getJoinCondition());
		return indexJoin.doIndexLoopJoin();
	}

	@Override
	public CreateTable evalSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJoinName() {
		// TODO Auto-generated method stub
		return null;
	}

}
