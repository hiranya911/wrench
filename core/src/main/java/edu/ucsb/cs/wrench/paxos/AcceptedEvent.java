package edu.ucsb.cs.wrench.paxos;

public class AcceptedEvent extends PaxosEvent {

    private long requestNumber;

    public AcceptedEvent(BallotNumber ballotNumber, long requestNumber) {
        super(ballotNumber);
        this.requestNumber = requestNumber;
    }

    public long getRequestNumber() {
        return requestNumber;
    }
}
