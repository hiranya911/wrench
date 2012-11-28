package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.commands.Command;

import java.util.Map;

public class AckEvent extends PaxosEvent {

    private RequestHistory history;

    public AckEvent(BallotNumber ballotNumber, RequestHistory history) {
        super(ballotNumber);
        this.history = history;
    }

    public Map<Long, BallotNumber> getAcceptNumbers() {
        return history.getPrevBallots();
    }

    public Map<Long, Command> getAcceptValues() {
        return history.getPrevCommands();
    }

    public Map<Long, Command> getPastOutcomes() {
        return history.getPrevOutcomes();
    }

    public void addAcceptNumber(long requestNumber, BallotNumber ballotNumber) {
        history.addPreviousBallotNumber(requestNumber, ballotNumber);
    }

    public void addAcceptValue(long requestNumber, Command command) {
        history.addPreviousCommand(requestNumber, command);
    }

    public void addOutcome(long requestNumber, Command command) {
        history.addPreviousOutcome(requestNumber, command);
    }
}
