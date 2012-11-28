package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.commands.Command;

public class DecideEvent extends PaxosEvent {

    private long requestNumber;
    private Command command;

    public DecideEvent(long requestNumber, Command command) {
        super(null);
        this.requestNumber = requestNumber;
        this.command = command;
    }

    public DecideEvent(BallotNumber ballotNumber, long requestNumber, Command command) {
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
