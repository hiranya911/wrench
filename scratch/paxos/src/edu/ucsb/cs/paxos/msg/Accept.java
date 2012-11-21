package edu.ucsb.cs.paxos.msg;

import edu.ucsb.cs.paxos.BallotNumber;
import edu.ucsb.cs.paxos.Message;
import edu.ucsb.cs.paxos.PaxosAgent;
import edu.ucsb.cs.paxos.log.LogEntry;

public class Accept extends Message {

    private int requestNumber;
    private BallotNumber ballotNumber;
    private LogEntry value;

    public Accept(PaxosAgent sender, int requestNumber, BallotNumber ballotNumber, LogEntry value) {
        super(sender);
        this.requestNumber = requestNumber;
        this.ballotNumber = ballotNumber;
        this.value = value;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public BallotNumber getBallotNumber() {
        return ballotNumber;
    }

    public LogEntry getValue() {
        return value;
    }
}
