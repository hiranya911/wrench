package edu.ucsb.cs.wrench.paxos;

public abstract class PaxosEvent {

    protected BallotNumber ballotNumber;

    protected PaxosEvent(BallotNumber ballotNumber) {
        this.ballotNumber = ballotNumber;
    }

    public BallotNumber getBallotNumber() {
        return ballotNumber;
    }
}
