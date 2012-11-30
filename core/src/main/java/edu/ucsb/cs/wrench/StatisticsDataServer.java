package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.TxCommitCommand;
import edu.ucsb.cs.wrench.commands.TxPrepareCommand;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.paxos.ZKPaxosAgent;

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
}
