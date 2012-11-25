package edu.ucsb.cs.wrench.messaging;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
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
import java.util.concurrent.ConcurrentHashMap;

public class WrenchCommunicator {

    private static final Log log = LogFactory.getLog(WrenchCommunicator.class);

    private Map<Member,TTransport> transports = new ConcurrentHashMap<Member, TTransport>();

    public static final String NULL_STRING = "null";

    public synchronized boolean sendElectionMessage(Member remoteMember) {
        try {
            WrenchManagementService.Client client = getClient(remoteMember);
            return client.election();
        } catch (TException e) {
            handleException(remoteMember, e);
            return !(e instanceof TTransportException);
        }
    }

    public void sendVictoryMessage(Member localMember, Member remoteMember) {
        try {
            WrenchManagementService.Client client = getClient(remoteMember);
            client.victory(localMember.getProcessId());
        } catch (TException e) {
            handleException(remoteMember, e);
        }
    }

    public boolean sendLeaderQueryMessage(Member remoteMember) {
        try {
            WrenchManagementService.Client client = getClient(remoteMember);
            return client.isLeader();
        } catch (TException e) {
            handleException(remoteMember, e);
            return false;
        }
    }

    public boolean ping(Member remoteMember) {
        try {
            WrenchManagementService.Client client = getClient(remoteMember);
            return client.ping();
        } catch (TException e) {
            handleException(remoteMember, e);
            return false;
        }
    }

    public void sendPrepare(BallotNumber ballotNumber, long requestNumber) {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        for (Member member : config.getMembers()) {
            try {
                log.info("Sending PREPARE message to: " + member.getProcessId());
                WrenchManagementService.Client client = getClient(member);
                client.prepare(toThriftBallotNumber(ballotNumber), requestNumber);
            } catch (TException e) {
                handleException(member, e);
            }
        }
    }

    public void sendAccept(BallotNumber ballotNumber, long requestNumber, Command value) {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        for (Member member : config.getMembers()) {
            try {
                WrenchManagementService.Client client = getClient(member);
                client.accept(toThriftBallotNumber(ballotNumber), requestNumber, value.toString());
            } catch (TException e) {
                handleException(member, e);
            }
        }
    }

    public void sendAccepted(BallotNumber ballotNumber, long requestNumber, Member member) {
        try {
            WrenchManagementService.Client client = getClient(member);
            client.accepted(toThriftBallotNumber(ballotNumber), requestNumber);
        } catch (TException e) {
            handleException(member, e);
        }
    }

    public void sendAck(AckEvent ack, Member member) {
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

        try {
            WrenchManagementService.Client client = getClient(member);
            client.ack(toThriftBallotNumber(ack.getBallotNumber()), acceptNum, acceptVal, outcomes);
        } catch (TException e) {
            handleException(member, e);
        }
    }

    public void sendDecide(BallotNumber ballotNumber, long requestNumber, Command command) {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        for (Member member : config.getMembers()) {
            if (member.isLocal()) {
                continue;
            }
            try {
                WrenchManagementService.Client client = getClient(member);
                client.decide(toThriftBallotNumber(ballotNumber), requestNumber, command.toString());
            } catch (TException e) {
                handleException(member, e);
            }
        }
    }

    private edu.ucsb.cs.wrench.messaging.BallotNumber toThriftBallotNumber(BallotNumber bal) {
        if (bal == null) {
            return new edu.ucsb.cs.wrench.messaging.BallotNumber(-1, NULL_STRING);
        } else {
            return new edu.ucsb.cs.wrench.messaging.BallotNumber(bal.getNumber(), bal.getProcessId());
        }
    }

    private WrenchManagementService.Client getClient(Member member) throws TTransportException {
        TTransport transport = transports.get(member);
        if (transport == null) {
            synchronized (this) {
                transport = transports.get(member);
                if (transport == null) {
                    transport = new TSocket(member.getHostname(), member.getPort());
                    transport.open();
                    log.info("Opened connection to: " + member.getProcessId());
                    transports.put(member, transport);
                }
            }
        }
        TProtocol protocol = new TBinaryProtocol(transport);
        return new WrenchManagementService.Client(protocol);
    }

    private void handleException(Member target, TException e) {
        String msg = "Error contacting the remote member: " + target.getProcessId();
        log.warn(msg);
        log.debug(msg, e);
        TTransport transport = transports.remove(target);
        if (transport != null && transport.isOpen()) {
            log.info("Closing connection to: " + target.getProcessId());
            transport.close();
        }
    }
}
