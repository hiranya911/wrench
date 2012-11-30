package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.config.Member;

import java.util.Map;

public interface AgentService {

    public void onElection();

    public void onVictory(Member member);

    public boolean onLeaderQuery();

    public void enqueue(PaxosEvent event);

    public boolean executeClientRequest(Command command);

    public Map<Long,Command> getPastOutcomes(long request);

    public BallotNumber getNextBallotNumber();

}
