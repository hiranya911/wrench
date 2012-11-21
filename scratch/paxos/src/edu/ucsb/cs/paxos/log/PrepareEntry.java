package edu.ucsb.cs.paxos.log;

public class PrepareEntry extends LogEntry {

    public PrepareEntry(String transactionId) {
        super(transactionId);
    }

    @Override
    public String toString() {
        return "[PREPARE Tx " + transactionId + "]";
    }
}
