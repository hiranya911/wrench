package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.paxos.PaxosAgent;

import java.io.File;

public class Main {

    public static void main(String[] args) {
        PaxosAgent agent = new PaxosAgent();
        agent.start();
    }
}
