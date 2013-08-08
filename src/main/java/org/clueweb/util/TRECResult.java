package org.clueweb.util;

public class TRECResult {

	public int qid;
	public String did;
	public int rank;
	public double score;
	public String record;//the entire TREC result line
	
	public TRECResult(int qid, String did, int rank, double score, String record) {
		this.qid = qid;
		this.did = did;
		this.rank = rank;
		this.score = score;
		this.record = record;
	}
}
