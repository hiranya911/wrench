package edu.ucsb.cs.wrench.paxos;

public class BallotNumber implements Comparable<BallotNumber> {

    private long number;
    private String processId;

    public BallotNumber(long number, String processId) {
        this.number = number;
        this.processId = processId;
    }

    public BallotNumber(String str) {
        str = str.substring(1, str.lastIndexOf(']'));
        String[] segments = str.split(":");
        this.number = Long.parseLong(segments[0]);
        this.processId = segments[1];
    }

    public void increment() {
        this.number++;
    }

    @Override
    public int compareTo(BallotNumber o) {
        int diff = (int) (number - o.number);
        if (diff != 0) {
            return diff;
        } else {
            return processId.compareTo(o.processId);
        }
    }

    @Override
    public String toString() {
        return "[" + number + ":" + processId + "]";
    }
}
