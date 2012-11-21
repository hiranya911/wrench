package edu.ucsb.cs.paxos;

public class BallotNumber implements Comparable<BallotNumber> {

    public int processId;
    public int proposalNumber;

    public BallotNumber(int processId, int proposalNumber) {
        this.processId = processId;
        this.proposalNumber = proposalNumber;
    }

    @Override
    public int compareTo(BallotNumber o) {
        if (proposalNumber > o.proposalNumber) {
            return 1;
        } else if (proposalNumber == o.proposalNumber &&
                processId > o.processId) {
            return 1;
        } else if (proposalNumber == o.proposalNumber &&
                processId == o.processId) {
            return 0;
        }
        return -1;
    }

    @Override
    public String toString() {
        return "<" + proposalNumber + "," + processId + ">";
    }

}
