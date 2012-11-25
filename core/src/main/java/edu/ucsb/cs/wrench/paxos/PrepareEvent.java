package edu.ucsb.cs.wrench.paxos;

public class PrepareEvent extends PaxosEvent {

    private long requestNumber;

    public PrepareEvent(BallotNumber ballotNumber, long requestNumber) {
        super(ballotNumber);
        this.requestNumber = requestNumber;
    }

    public long getRequestNumber() {
        return requestNumber;
    }
}
