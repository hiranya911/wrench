package edu.ucsb.cs.paxos;

import edu.ucsb.cs.paxos.msg.Append;

import java.util.UUID;

public class Client {

    private PaxosAgent leader;

    public Client(PaxosAgent leader) {
        this.leader = leader;
    }

    public void sendAppend() {
        //Network.send(leader, new Append(null, UUID.randomUUID().toString()));
        leader.append(new Append(null, UUID.randomUUID().toString()));
    }
}
