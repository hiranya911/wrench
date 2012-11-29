package edu.ucsb.cs.wrench.messaging;

import edu.ucsb.cs.wrench.WrenchException;
import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.CommandFactory;
import edu.ucsb.cs.wrench.commands.TxPrepareCommand;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.paxos.AcceptedEvent;
import edu.ucsb.cs.wrench.paxos.AckEvent;
import edu.ucsb.cs.wrench.paxos.BallotNumber;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;

public class WrenchCommunicator {

    private static final Log log = LogFactory.getLog(WrenchCommunicator.class);

    public static final String NULL_STRING = "null";

    public synchronized boolean sendElectionMessage(Member remoteMember) {
        TTransport transport = new TSocket(remoteMember.getHostname(), remoteMember.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            return client.election();
        } catch (TException e) {
            handleException(remoteMember, e);
            return !(e instanceof TTransportException);
        } finally {
            close(transport);
        }
    }

    public void sendVictoryMessage(Member localMember, Member remoteMember) {
        TTransport transport = new TSocket(remoteMember.getHostname(), remoteMember.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            client.victory(localMember.getProcessId());
        } catch (TException e) {
            handleException(remoteMember, e);
        } finally {
            close(transport);
        }
    }

    public boolean sendLeaderQueryMessage(Member remoteMember) {
        TTransport transport = new TSocket(remoteMember.getHostname(), remoteMember.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            return client.isLeader();
        } catch (TException e) {
            handleException(remoteMember, e);
            return false;
        } finally {
            close(transport);
        }
    }

    public boolean ping(Member remoteMember) {
        TTransport transport = new TSocket(remoteMember.getHostname(), remoteMember.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            return client.ping();
        } catch (TException e) {
            handleException(remoteMember, e);
            return false;
        } finally {
            close(transport);
        }
    }

    public void sendPrepare(BallotNumber ballotNumber, long requestNumber) {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        for (Member member : config.getMembers()) {
            TTransport transport = new TSocket(member.getHostname(), member.getPort());
            try {
                log.info("Sending PREPARE message to: " + member.getProcessId());
                WrenchManagementService.Client client = getClient(transport);
                client.prepare(toThriftBallotNumber(ballotNumber), requestNumber);
            } catch (TException e) {
                handleException(member, e);
            } finally {
                close(transport);
            }
        }
    }

    public void sendAccept(BallotNumber ballotNumber, long requestNumber, Command value) {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        for (Member member : config.getMembers()) {
            TTransport transport = new TSocket(member.getHostname(), member.getPort());
            try {
                WrenchManagementService.Client client = getClient(transport);
                client.accept(toThriftBallotNumber(ballotNumber), requestNumber, value.toString());
            } catch (TException e) {
                handleException(member, e);
            } finally {
                close(transport);
            }
        }
    }

    public void sendAccepted(AcceptedEvent accepted, Member remoteMember) {
        TTransport transport = new TSocket(remoteMember.getHostname(), remoteMember.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            client.accepted(toThriftBallotNumber(accepted.getBallotNumber()),
                    accepted.getRequestNumber());
        } catch (TException e) {
            handleException(remoteMember, e);
        } finally {
            close(transport);
        }
    }

    public void sendAck(AckEvent ack, Member remoteMember) {
        Map<Long,edu.ucsb.cs.wrench.messaging.BallotNumber> acceptNum =
                new HashMap<Long, edu.ucsb.cs.wrench.messaging.BallotNumber>();
        for (Map.Entry<Long,BallotNumber> entry : ack.getAcceptNumbers().entrySet()) {
            acceptNum.put(entry.getKey(), toThriftBallotNumber(entry.getValue()));
        }

        Map<Long,String> acceptVal = new HashMap<Long, String>();
        for (Map.Entry<Long,Command> entry : ack.getAcceptValues().entrySet()) {
            acceptVal.put(entry.getKey(), entry.getValue().toString());
        }

        Map<Long,String> outcomes = new HashMap<Long, String>();
        for (Map.Entry<Long,Command> entry : ack.getPastOutcomes().entrySet()) {
            outcomes.put(entry.getKey(), entry.getValue().toString());
        }

        TTransport transport = new TSocket(remoteMember.getHostname(), remoteMember.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            client.ack(toThriftBallotNumber(ack.getBallotNumber()), acceptNum, acceptVal, outcomes);
        } catch (TException e) {
            handleException(remoteMember, e);
        } finally {
            close(transport);
        }
    }

    public void sendDecide(BallotNumber ballotNumber, long requestNumber, Command command) {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        for (Member member : config.getMembers()) {
            if (member.isLocal()) {
                continue;
            }

            TTransport transport = new TSocket(member.getHostname(), member.getPort());
            try {
                WrenchManagementService.Client client = getClient(transport);
                client.decide(toThriftBallotNumber(ballotNumber), requestNumber, command.toString());
            } catch (TException e) {
                handleException(member, e);
            } finally {
                close(transport);
            }
        }
    }

    public boolean relayCommand(Command command, Member member) {
        TTransport transport = new TSocket(member.getHostname(), member.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            if (command instanceof TxPrepareCommand) {
                TxPrepareCommand prepare = (TxPrepareCommand) command;
                return client.append(prepare.getTransactionId(), prepare.getData());
            } else {
                return false;
            }
        } catch (TTransportException e) {
            handleException(member, e);
            throw new WrenchException("Error contacting the member: " + member.getProcessId(), e);
        } catch (TException e) {
            handleException(member, e);
            return false;
        } finally {
            close(transport);
        }
    }

    public Map<Long,Command> getPastOutcomes(long lastRequest, Member member) {
        TTransport transport = new TSocket(member.getHostname(), member.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            Map<Long,String> commands = client.getPastOutcomes(lastRequest);
            Map<Long,Command> pastOutcomes = new HashMap<Long, Command>();
            for (Map.Entry<Long,String> entry : commands.entrySet()) {
                pastOutcomes.put(entry.getKey(), CommandFactory.createCommand(entry.getValue()));
            }
            return pastOutcomes;
        } catch (TException e) {
            handleException(member, e);
            throw new WrenchException("Error contacting the remote member", e);
        } finally {
            close(transport);
        }
    }

    public BallotNumber getNextBallotNumber(Member member) {
        TTransport transport = new TSocket(member.getHostname(), member.getPort());
        try {
            WrenchManagementService.Client client = getClient(transport);
            edu.ucsb.cs.wrench.messaging.BallotNumber ballot = client.getNextBallot();
            if (ballot.getNumber() == -1 && ballot.getProcessId().equals(NULL_STRING)) {
                return null;
            }
            return new BallotNumber(ballot.getNumber(), ballot.getProcessId());
        } catch (TException e) {
            handleException(member, e);
            throw new WrenchException("Error contacting the remote member", e);
        } finally {
            close(transport);
        }
    }

    private edu.ucsb.cs.wrench.messaging.BallotNumber toThriftBallotNumber(BallotNumber bal) {
        if (bal == null) {
            return new edu.ucsb.cs.wrench.messaging.BallotNumber(-1, NULL_STRING);
        } else {
            return new edu.ucsb.cs.wrench.messaging.BallotNumber(bal.getNumber(), bal.getProcessId());
        }
    }

    private WrenchManagementService.Client getClient(TTransport transport) throws TTransportException {
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new WrenchManagementService.Client(protocol);
    }

    private void close(TTransport transport) {
        if (transport.isOpen()) {
            transport.close();
        }
    }

    private void handleException(Member target, TException e) {
        String msg = "Error contacting the remote member: " + target.getProcessId();
        log.warn(msg);
        log.debug(msg, e);
    }
}
