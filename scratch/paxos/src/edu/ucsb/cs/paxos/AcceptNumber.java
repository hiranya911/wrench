package edu.ucsb.cs.paxos;

import edu.ucsb.cs.paxos.log.LogEntry;

import java.util.HashMap;
import java.util.Map;


public class AcceptNumber {

    private Map<Integer,Acceptance> acceptanceMap = new HashMap<Integer, Acceptance>();

    public void put(int requestNumber, BallotNumber acceptNumber, LogEntry value) {
        acceptanceMap.put(requestNumber, new Acceptance(acceptNumber, value));
    }

    public Acceptance get(int requestNumber) {
        return acceptanceMap.get(requestNumber);
    }

    public Integer[] getRequestNumbers() {
        return acceptanceMap.keySet().toArray(new Integer[acceptanceMap.size()]);
    }

    class Acceptance {
        private BallotNumber acceptNum;
        private LogEntry acceptVal;

        private Acceptance(BallotNumber acceptNum, LogEntry acceptVal) {
            this.acceptNum = acceptNum;
            this.acceptVal = acceptVal;
        }

        public BallotNumber getAcceptNum() {
            return acceptNum;
        }

        public LogEntry getAcceptVal() {
            return acceptVal;
        }
    }
}
