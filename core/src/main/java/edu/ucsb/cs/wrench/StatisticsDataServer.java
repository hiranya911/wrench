package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.TxCommitCommand;
import edu.ucsb.cs.wrench.commands.TxPrepareCommand;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.paxos.DatabaseSnapshot;
import edu.ucsb.cs.wrench.paxos.ZKPaxosAgent;

import java.util.List;

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

    public StatisticsDataServer() {
        WrenchConfiguration.getConfiguration().setDataFileName("STATS.txt");
    }

    @Override
    public void onDecision(long requestNumber, Command command) {
        if (!leader.isLocal()) {
            return;
        }

        if (command instanceof TxPrepareCommand) {
            final TxPrepareCommand prepare = (TxPrepareCommand) command;
            if (!pendingTransactions.contains(prepare.getTransactionId())) {
                synchronized (pendingTransactions) {
                    try {
                        log.info("Waiting for pending tx to arrive");
                        pendingTransactions.wait(10000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            if (pendingTransactions.remove(prepare.getTransactionId())) {
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        executeClientRequest(new TxCommitCommand(prepare.getTransactionId()));
                    }
                });
            } else {
                throw new WrenchException("Never heard of this transaction from the other cluster");
            }
        } else if (command instanceof TxCommitCommand) {
            TxCommitCommand commit = (TxCommitCommand) command;
            while (true) {
                for (Member peer : WrenchConfiguration.getConfiguration().getPeers()) {
                    try {
                        boolean done = communicator.notifyCommit(commit.getTransactionId(),
                                commit.getLineNumber(), peer);
                        if (done) {
                            return;
                        }
                        log.info("Notified peer: " + peer.getProcessId() + " about COMMIT " +
                                commit.getTransactionId());
                        break;
                    } catch (WrenchException e) {
                        log.error("Error while contacting remote peer", e);
                    }
                }
            }
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
