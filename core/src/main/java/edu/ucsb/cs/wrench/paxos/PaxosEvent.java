package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.messaging.Event;

public abstract class PaxosEvent extends Event {

    protected BallotNumber ballotNumber;

    protected PaxosEvent(BallotNumber ballotNumber) {
        this.ballotNumber = ballotNumber;
    }

    public BallotNumber getBallotNumber() {
        return ballotNumber;
    }
}
