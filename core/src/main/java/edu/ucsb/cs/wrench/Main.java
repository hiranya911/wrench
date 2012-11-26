package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.paxos.PaxosAgent;

import java.io.File;

public class Main {

    public static void main(String[] args) {
        final PaxosAgent agent = new PaxosAgent();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                agent.stop();
            }
        });
        agent.start();
    }
}
