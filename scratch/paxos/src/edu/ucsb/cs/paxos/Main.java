package edu.ucsb.cs.paxos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args) {

        int size = 10;
        PaxosAgent[] agents = new PaxosAgent[size];
        for (int i = 0; i < size; i++) {
            agents[i] = new PaxosAgent(i);
        }
        Network.init(agents);

        ExecutorService pool = Executors.newFixedThreadPool(size);
        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < size; i++) {
            futures.add(pool.submit(agents[i]));
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }

        for (int i = 0; i < 10; i++) {
            Worker w = new Worker(new Client(agents[0]));
            w.start();
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {

        }

        for (int i = 1; i < agents.length; i++) {
            if (!agents[i].getLog().toString().equals(agents[0].getLog().toString())) {
                System.err.println("ERROR\n" + agents[i].getLog() + "\n" + agents[0].getLog());
                System.exit(1);
            }
        }
        System.out.println("MATCH");
        System.exit(0);
    }

    private static class Worker extends Thread {

        private Client client;

        private Worker(Client client) {
            this.client = client;
        }

        public void run() {
            for (int i = 0; i < 20; i++) {
                client.sendAppend();
            }
        }

    }
}
