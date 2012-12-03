package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.TxCommitCommand;
import edu.ucsb.cs.wrench.commands.TxPrepareCommand;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.paxos.DatabaseSnapshot;
import edu.ucsb.cs.wrench.paxos.ZKPaxosAgent;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StatisticsDataServer extends ZKPaxosAgent {

    public static void main(String[] args) {
        final StatisticsDataServer server = new StatisticsDataServer();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.stop();
            }
        });
        server.start();
    }

    protected final Set<String> pendingTransactions = Collections.synchronizedSet(new HashSet<String>());

    public StatisticsDataServer() {
        config.setDataFileName("STATS.txt");
    }

    @Override
    public void onDecision(long requestNumber, Command command) {
        if (!leader.isLocal()) {
            return;
        }
        if (command instanceof TxPrepareCommand) {
            final TxPrepareCommand prepare = (TxPrepareCommand) command;
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    executeClientRequest(new TxCommitCommand(prepare.getTransactionId()));
                }
            });
        } else if (command instanceof TxCommitCommand) {
            TxCommitCommand commit = (TxCommitCommand) command;
            while (true) {
                for (Member peer : config.getPeers()) {
                    try {
                        boolean done = communicator.notifyCommit(commit.getTransactionId(),
                                commit.getLineNumber(), peer);
                        if (done) {
                            if (log.isDebugEnabled()) {
                                log.debug("Notified peer: " + peer.getProcessId() + " about COMMIT " +
                                        commit.getTransactionId());
                            }
                            pendingTransactions.remove(commit.getTransactionId());
                            return;
                        }
                    } catch (WrenchException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Error while contacting remote peer " + peer.getProcessId(), e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean executeClientRequest(Command command) {
        if (leader.isLocal() && command instanceof TxPrepareCommand) {
            if (!pendingTransactions.contains(command.getTransactionId())) {
                return false;
            }
        }
        return super.executeClientRequest(command);
    }

    public void onAppendNotification(String transactionId) {
        if (log.isDebugEnabled()) {
            log.debug("Received prepare notification for " + transactionId);
        }

        if (leader.isLocal()) {
            pendingTransactions.add(transactionId);
        } else {
            communicator.notifyPrepare(transactionId, leader);
        }
    }

    @Override
    public DatabaseSnapshot readSnapshot() {
        String[] stats = getLines();
        if (stats.length == 0) {
            return new DatabaseSnapshot();
        } else {
            List<String> grades = null;
            for (Member peer : config.getPeers()) {
                try {
                    grades = communicator.getLines(stats.length, peer);
                } catch (WrenchException e) {
                    log.error("Error while obtaining data from peer", e);
                }
            }

            if (grades != null && grades.size() > 0) {
                int actualLineCount = Math.min(stats.length, grades.size());
                DatabaseSnapshot snapshot = new DatabaseSnapshot();
                for (int i = 0; i < actualLineCount; i++) {
                    snapshot.addData(grades.get(i), stats[i]);
                }
                return snapshot;
            }
        }
        return new DatabaseSnapshot();
    }
}
