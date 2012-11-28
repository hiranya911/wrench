package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.commands.Command;

import java.util.HashMap;
import java.util.Map;

public class RequestHistory {

    private Map<Long,BallotNumber> prevBallots = new HashMap<Long, BallotNumber>();
    private Map<Long,Command> prevCommands = new HashMap<Long, Command>();
    private Map<Long,Command> prevOutcomes = new HashMap<Long, Command>();

    public void addPreviousBallotNumber(long request, BallotNumber ballot) {
        prevBallots.put(request, ballot);
    }

    public void addPreviousCommand(long request, Command command) {
        prevCommands.put(request, command);
    }

    public void addPreviousOutcome(long request, Command command) {
        prevOutcomes.put(request, command);
    }

    public Map<Long, BallotNumber> getPrevBallots() {
        return prevBallots;
    }

    public Map<Long, Command> getPrevCommands() {
        return prevCommands;
    }

    public Map<Long, Command> getPrevOutcomes() {
        return prevOutcomes;
    }
}
