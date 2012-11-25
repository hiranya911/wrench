package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.commands.Command;

import java.util.HashMap;
import java.util.Map;

public class AckEvent extends PaxosEvent {

    private Map<Long,BallotNumber> acceptNumbers = new HashMap<Long, BallotNumber>();
    private Map<Long,Command> acceptValues = new HashMap<Long, Command>();
    private Map<Long,Command> pastOutcomes = new HashMap<Long, Command>();

    public AckEvent(BallotNumber ballotNumber) {
        super(ballotNumber);
    }

    public AckEvent(BallotNumber ballotNumber, Map<Long, BallotNumber> acceptNumbers,
                    Map<Long, Command> acceptValues, Map<Long, Command> pastOutcomes) {
        super(ballotNumber);
        this.acceptNumbers = acceptNumbers;
        this.acceptValues = acceptValues;
        this.pastOutcomes = pastOutcomes;
    }

    public void addAcceptNumber(long n, BallotNumber bal) {
        acceptNumbers.put(n, bal);
    }

    public void addAcceptValue(long n, Command c) {
        acceptValues.put(n, c);
    }

    public void addOutcome(long n, Command c) {
        pastOutcomes.put(n, c);
    }

    public Map<Long, BallotNumber> getAcceptNumbers() {
        return acceptNumbers;
    }

    public Map<Long, Command> getAcceptValues() {
        return acceptValues;
    }

    public Map<Long, Command> getPastOutcomes() {
        return pastOutcomes;
    }
}
