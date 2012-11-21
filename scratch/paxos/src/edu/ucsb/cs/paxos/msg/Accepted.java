package edu.ucsb.cs.paxos.msg;

import edu.ucsb.cs.paxos.BallotNumber;
import edu.ucsb.cs.paxos.PaxosAgent;
import edu.ucsb.cs.paxos.log.LogEntry;

public class Accepted extends Accept {

    public Accepted(PaxosAgent sender, int requestNumber,
                    BallotNumber ballotNumber, LogEntry value) {
        super(sender, requestNumber, ballotNumber, value);
    }
}
