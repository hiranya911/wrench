package edu.ucsb.cs.wrench.messaging;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.CommandFactory;
import edu.ucsb.cs.wrench.commands.TxPrepareCommand;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.paxos.*;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.TreeMap;

public class WrenchManagementServiceHandler implements WrenchManagementService.Iface {

    private AgentService agent;

    public WrenchManagementServiceHandler(AgentService agent) {
        this.agent = agent;
    }

    @Override
    public boolean ping() throws TException {
        return true;
    }

    @Override
    public boolean election() throws TException {
        agent.onElection();
        return true;
    }

    @Override
    public void victory(String processId) throws TException {
        agent.onVictory(WrenchConfiguration.getConfiguration().getMember(processId));
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

        AckEvent ack = new AckEvent(toPaxosBallotNumber(bal), new RequestHistory());
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

    @Override
    public boolean append(String transactionId, String data) throws TException {
        TxPrepareCommand command = new TxPrepareCommand(transactionId, data);
        return agent.executeClientRequest(command);
    }

    @Override
    public void notifyPrepare(String transactionId) throws TException {
        agent.onAppendNotification(transactionId);
    }

    @Override
    public boolean notifyCommit(String transactionId, long lineNumber) throws TException {
        return agent.onAppendCommit(transactionId, lineNumber);
    }

    @Override
    public Map<Long,String> getPastOutcomes(long lastRequest) throws TException {
        Map<Long,Command> commands = agent.getPastOutcomes(lastRequest);
        Map<Long,String> pastOutcomes = new TreeMap<Long, String>();
        for (Map.Entry<Long,Command> entry : commands.entrySet()) {
            pastOutcomes.put(entry.getKey(), entry.getValue().toString());
        }
        return pastOutcomes;
    }

    @Override
    public BallotNumber getNextBallot() throws TException {
        edu.ucsb.cs.wrench.paxos.BallotNumber ballotNumber = agent.getNextBallotNumber();
        if (ballotNumber == null) {
            return new BallotNumber(-1, WrenchCommunicator.NULL_STRING);
        }
        return new BallotNumber(ballotNumber.getNumber(), ballotNumber.getProcessId());
    }

    private edu.ucsb.cs.wrench.paxos.BallotNumber toPaxosBallotNumber(BallotNumber ballotNumber) {
        if (ballotNumber.number == -1 && ballotNumber.processId.equals(WrenchCommunicator.NULL_STRING)) {
            return null;
        }
        return new edu.ucsb.cs.wrench.paxos.BallotNumber(ballotNumber.number,
                ballotNumber.processId);
    }
}
