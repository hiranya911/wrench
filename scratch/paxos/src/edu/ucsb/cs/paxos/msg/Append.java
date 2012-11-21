package edu.ucsb.cs.paxos.msg;

import edu.ucsb.cs.paxos.Message;
import edu.ucsb.cs.paxos.PaxosAgent;

public class Append extends Message {

    private String transactionId;

    public Append(PaxosAgent sender, String transactionId) {
        super(sender);
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }
}
