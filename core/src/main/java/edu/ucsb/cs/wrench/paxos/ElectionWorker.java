package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.messaging.WrenchCommunicator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ElectionWorker implements Runnable {

    private static final Log log = LogFactory.getLog(ElectionWorker.class);

    private WrenchCommunicator communicator;
    private WrenchConfiguration config;
    private boolean victoryMessageReceived;

    public ElectionWorker(WrenchCommunicator communicator) {
        this.communicator = communicator;
        this.config = WrenchConfiguration.getConfiguration();
    }

    @Override
    public void run() {
        victoryMessageReceived = false;
        log.info("Starting new leader election");
        Member[] members = config.getMembers();
        Arrays.sort(members);

        Set<Member> highProcesses = new HashSet<Member>();
        Member localMember = null;
        for (Member member : members) {
            if (member.isLocal()) {
                localMember = member;
            } else if (localMember != null) {
                highProcesses.add(member);
            }
        }

        if (highProcesses.isEmpty()) {
            log.info("I'm the highest process of all - I Win");
            sendVictoryNotification(localMember);
        } else {
            while (true) {
                boolean higherProcessSeen = false;
                for (Member member : members) {
                    if (communicator.sendElectionMessage(member)) {
                        higherProcessSeen = true;
                        break;
                    }
                }

                if (higherProcessSeen) {
                    log.info("Some higher process seen - Waiting for the victory notification");
                    synchronized (this) {
                        try {
                            this.wait(config.getLeaderElectionTimeout());
                        } catch (InterruptedException e) {
                            log.error("Unexpected interrupt", e);
                        }
                    }

                    if (!victoryMessageReceived) {
                        log.info("No victory notification received - Retrying...");
                    } else {
                        log.info("Victory message received - This election is done and dusted");
                        break;
                    }
                } else {
                    log.info("I'm the highest process alive - I win");
                    sendVictoryNotification(localMember);
                    break;
                }
            }
        }
    }

    public void setVictoryMessageReceived(boolean victoryMessageReceived) {
        this.victoryMessageReceived = victoryMessageReceived;
        synchronized (this) {
            this.notifyAll();
        }
    }

    private void sendVictoryNotification(Member localMember) {
        for (Member member : config.getMembers()) {
            if (member.compareTo(localMember) <= 0) {
                communicator.sendVictoryMessage(localMember, member);
            }
        }
    }
}
