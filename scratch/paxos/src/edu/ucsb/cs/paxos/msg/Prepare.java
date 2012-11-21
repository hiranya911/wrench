package edu.ucsb.cs.paxos.msg;

import edu.ucsb.cs.paxos.BallotNumber;
import edu.ucsb.cs.paxos.Message;
import edu.ucsb.cs.paxos.PaxosAgent;

public class Prepare extends Message {

    private BallotNumber ballotNumber;

    public Prepare(PaxosAgent sender, BallotNumber ballotNumber) {
        super(sender);
        this.sender = sender;
        this.ballotNumber = ballotNumber;
    }

    public BallotNumber getBallotNumber() {
        return ballotNumber;
    }
}
