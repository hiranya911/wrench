package edu.ucsb.cs.paxos.log;

public class CommitEntry extends LogEntry {

    private int lineNumber;

    public CommitEntry(String transactionId, int lineNumber) {
        super(transactionId);
        this.lineNumber = lineNumber;
    }

    public int getLineNumber() {
        return lineNumber;
    }
}
