package edu.ucsb.cs.wrench.messaging;

import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.paxos.PaxosAgent;
import org.apache.thrift.TException;

public class WrenchManagementServiceHandler implements WrenchManagementService.Iface {

    private PaxosAgent agent;
    private WrenchConfiguration config;

    public WrenchManagementServiceHandler(PaxosAgent agent) {
        this.agent = agent;
        this.config = WrenchConfiguration.getConfiguration();
    }

    @Override
    public boolean election() throws TException {
        agent.onElection();
        return true;
    }

    @Override
    public void victory(String processId) throws TException {
        Member member = config.getMember(processId);
        agent.onVictory(member);
    }

    @Override
    public boolean isLeader() throws TException {
        return agent.onLeaderQuery();
    }
}
