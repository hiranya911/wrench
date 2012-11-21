package edu.ucsb.cs.paxos.msg;

import edu.ucsb.cs.paxos.*;

public class PrepareAck extends Message {

    private BallotNumber ballotNumber;
    private AcceptNumber acceptNumber;

    public PrepareAck(PaxosAgent sender, BallotNumber ballotNumber, AcceptNumber acceptNumber) {
        super(sender);
        this.ballotNumber = ballotNumber;
        this.acceptNumber = acceptNumber;
    }

    public BallotNumber getBallotNumber() {
        return ballotNumber;
    }

    public AcceptNumber getAcceptNumber() {
        return acceptNumber;
    }
}
