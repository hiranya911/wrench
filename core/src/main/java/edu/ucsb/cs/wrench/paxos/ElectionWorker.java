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
        log.info("Starting new leader election");
        Member[] members = config.getMembers();
        victoryMessageReceived = false;
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
            waitForMajority();
            log.info("I'm the highest process of all - I Win");
            sendVictoryNotification(localMember);
        } else {
            while (true) {
                waitForMajority();
                boolean higherProcessSeen = false;
                for (Member member : highProcesses) {
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

    public Member discoverLeader() {
        waitForMajority();

        log.info("Checking if a leader exists");
        for (Member member : config.getMembers()) {
            if (!member.isLocal() && communicator.sendLeaderQueryMessage(member)) {
                log.info("Found existing leader: " + member.getProcessId());
                return member;
            }
        }
        return null;
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

    private void waitForMajority() {
        Member[] members = config.getMembers();
        while (true) {
            int liveNodes = 1;
            log.info("Checking if a majority of members are up");
            for (Member member : members) {
                if (member.isLocal()) {
                    continue;
                }
                if (communicator.ping(member)) {
                    liveNodes++;
                }
            }
            if (liveNodes > members.length / 2.0) {
                log.info("Majority is up");
                break;
            } else {
                log.info("Majority of members are unreachable - Waiting for more members...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
