package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.TxPrepareCommand;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.paxos.DatabaseSnapshot;
import edu.ucsb.cs.wrench.paxos.ZKPaxosAgent;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class GradesDataServer extends ZKPaxosAgent {

    public static void main(String[] args) {
        final GradesDataServer server = new GradesDataServer();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.stop();
            }
        });
        server.start();
    }

    public GradesDataServer() {
        config.setDataFileName("GRADES.txt");
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
                    for (int i = 0; i < 3; i++) {
                        try {
                            if (peerLeader == null) {
                                initPeerLeader();
                            }
                            communicator.notifyPrepare(prepare.getTransactionId(), peerLeader);
                            if (log.isDebugEnabled()) {
                                log.debug("Notified peer: " + peerLeader.getProcessId() +
                                        " about PREPARE " + prepare.getTransactionId());
                            }
                            return;
                        } catch (WrenchException e) {
                            initPeerLeader();
                        }
                    }
                }
            });
        }
    }

    private volatile Member peerLeader = null;

    private void initPeerLeader() {
        if (log.isDebugEnabled()) {
            log.debug("Starting peer leader discovery");
        }
        for (int i = 0; i < 3; i++) {
            for (Member peer : config.getPeers()) {
                if (communicator.sendLeaderQueryMessage(peer)) {
                    peerLeader = peer;
                    return;
                }
            }
            try {
                log.info("Unable to locate peer leader - Retrying in 2 seconds");
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public DatabaseSnapshot readSnapshot() {
        String[] grades = getLines();
        if (grades.length == 0) {
            return new DatabaseSnapshot();
        } else {
            List<String> stats = null;
            for (Member peer : config.getPeers()) {
                try {
                    stats = communicator.getLines(grades.length, peer);
                } catch (WrenchException e) {
                    log.error("Error while obtaining data from peer", e);
                }
            }

            if (stats != null && stats.size() > 0) {
                int actualLineCount = Math.min(grades.length, stats.size());
                DatabaseSnapshot snapshot = new DatabaseSnapshot();
                for (int i = 0; i < actualLineCount; i++) {
                    snapshot.addData(grades[i], stats.get(i));
                }
                return snapshot;
            }
        }
        return new DatabaseSnapshot();
    }
}
