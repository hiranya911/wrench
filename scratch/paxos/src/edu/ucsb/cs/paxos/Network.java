package edu.ucsb.cs.paxos;

public class Network {

    private static PaxosAgent[] agents;

    public static void init(PaxosAgent[] agents) {
        Network.agents = agents;
    }

    public static void broadcast(Message message) {
        for (PaxosAgent agent : agents) {
            agent.deliver(message);
        }
    }

    public static void send(PaxosAgent receiver, Message message) {
        receiver.deliver(message);
    }

    public static int getSize() {
        return agents.length;
    }
}
