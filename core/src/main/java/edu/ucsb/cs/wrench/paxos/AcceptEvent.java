package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.commands.Command;

public class AcceptEvent extends PaxosEvent {

    private long requestNumber;
    private Command command;

    public AcceptEvent(BallotNumber ballotNumber, long requestNumber, Command command) {
        super(ballotNumber);
        this.requestNumber = requestNumber;
        this.command = command;
    }

    public long getRequestNumber() {
        return requestNumber;
    }

    public Command getCommand() {
        return command;
    }
}
