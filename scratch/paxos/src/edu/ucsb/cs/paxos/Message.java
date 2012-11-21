package edu.ucsb.cs.paxos;

public abstract class Message {

    protected PaxosAgent sender;

    public Message(PaxosAgent sender) {
        this.sender = sender;
    }

    public PaxosAgent getSender() {
        return sender;
    }
}
