package edu.ucsb.cs.paxos.log;

public abstract class LogEntry {

    protected String transactionId;

    public LogEntry(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }
}
