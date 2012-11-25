package edu.ucsb.cs.wrench.messaging;

import edu.ucsb.cs.wrench.commands.CommandFactory;
import edu.ucsb.cs.wrench.elections.ElectionEvent;
import edu.ucsb.cs.wrench.elections.VictoryEvent;
import edu.ucsb.cs.wrench.paxos.*;
import org.apache.thrift.TException;

import java.util.Map;

public class WrenchManagementServiceHandler implements WrenchManagementService.Iface {

    private PaxosAgent agent;

    public WrenchManagementServiceHandler(PaxosAgent agent) {
        this.agent = agent;
    }

    @Override
    public boolean ping() throws TException {
        return true;
    }

    @Override
    public boolean election() throws TException {
        agent.enqueue(new ElectionEvent());
        return true;
    }

    @Override
    public void victory(String processId) throws TException {
        agent.enqueue(new VictoryEvent(processId));
    }

    @Override
    public boolean isLeader() throws TException {
        return agent.onLeaderQuery();
    }

    @Override
    public void prepare(BallotNumber bal, long requestNumber) throws TException {
        PrepareEvent prepare = new PrepareEvent(toPaxosBallotNumber(bal), requestNumber);
        agent.enqueue(prepare);
    }

    @Override
    public void ack(BallotNumber bal, Map<Long, BallotNumber> acceptNumbers,
                    Map<Long, String> acceptValues, Map<Long,String> outcomes) throws TException {

        AckEvent ack = new AckEvent(toPaxosBallotNumber(bal));
        for (Map.Entry<Long,BallotNumber> entry : acceptNumbers.entrySet()) {
            ack.addAcceptNumber(entry.getKey(), toPaxosBallotNumber(entry.getValue()));
        }

        for (Map.Entry<Long,String> entry : acceptValues.entrySet()) {
            ack.addAcceptValue(entry.getKey(), CommandFactory.createCommand(entry.getValue()));
        }

        for (Map.Entry<Long,String> entry : outcomes.entrySet()) {
            ack.addOutcome(entry.getKey(), CommandFactory.createCommand(entry.getValue()));
        }
        agent.enqueue(ack);
    }

    @Override
    public void accept(BallotNumber b, long requestNumber, String value) throws TException {
        AcceptEvent accept = new AcceptEvent(toPaxosBallotNumber(b), requestNumber,
                CommandFactory.createCommand(value));
        agent.enqueue(accept);
    }

    @Override
    public void accepted(BallotNumber bal, long requestNumber) throws TException {
        AcceptedEvent accepted = new AcceptedEvent(toPaxosBallotNumber(bal), requestNumber);
        agent.enqueue(accepted);
    }

    @Override
    public void decide(BallotNumber ballotNumber, long requestNumber, String command) throws TException {
        DecideEvent decide = new DecideEvent(toPaxosBallotNumber(ballotNumber),
                requestNumber, CommandFactory.createCommand(command));
        agent.enqueue(decide);
    }

    private edu.ucsb.cs.wrench.paxos.BallotNumber toPaxosBallotNumber(BallotNumber ballotNumber) {
        if (ballotNumber.number == -1 && ballotNumber.processId.equals(WrenchCommunicator.NULL_STRING)) {
            return null;
        }
        return new edu.ucsb.cs.wrench.paxos.BallotNumber(ballotNumber.number,
                ballotNumber.processId);
    }
}
