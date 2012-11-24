package edu.ucsb.cs.wrench.messaging;

import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;
import java.util.Map;

public class WrenchCommunicator {

    private static final Log log = LogFactory.getLog(WrenchCommunicator.class);

    private Map<Member,TTransport> transports = new HashMap<Member, TTransport>();

    public synchronized boolean sendElectionMessage(Member remoteMember) {
        TTransport transport = getTransport(remoteMember);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
            return client.election();
        } catch (TException e) {
            String msg = "Error contacting the remote member: " + remoteMember.getProcessId();
            log.warn(msg);
            log.debug(msg, e);
            return false;
        } finally {
            if (transport.isOpen()) {
                transport.close();
            }
        }
    }

    public synchronized void sendVictoryMessage(Member localMember, Member remoteMember) {
        TTransport transport = getTransport(remoteMember);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
            client.victory(localMember.getProcessId());
        } catch (TException e) {
            String msg = "Error contacting the remote member: " + remoteMember.getProcessId();
            log.warn(msg);
            log.debug(msg, e);
        } finally {
            if (transport.isOpen()) {
                transport.close();
            }
        }
    }

    public synchronized boolean sendLeaderQueryMessage(Member remoteMember) {
        TTransport transport = getTransport(remoteMember);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
            return client.isLeader();
        } catch (TException e) {
            String msg = "Error contacting the remote member: " + remoteMember.getProcessId();
            log.warn(msg);
            log.debug(msg, e);
            return false;
        } finally {
            if (transport.isOpen()) {
                transport.close();
            }
        }
    }

    private TTransport getTransport(Member member) {
        if (!transports.containsKey(member)) {
            synchronized (this) {
                if (!transports.containsKey(member)) {
                    TSocket transport = new TSocket(member.getHostname(), member.getPort());
                    transport.setTimeout(WrenchConfiguration.getConfiguration().
                            getLeaderElectionTimeout());
                    transports.put(member, transport);
                    return transport;
                }
            }
        }
        return transports.get(member);
    }
}
