package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.TxPrepareCommand;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.paxos.PaxosAgent;
import edu.ucsb.cs.wrench.paxos.ZKPaxosAgent;

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
        WrenchConfiguration.getConfiguration().setDataFileName("GRADES.txt");
    }

    @Override
    public void onDecision(long requestNumber, Command command) {
        if (!leader.isLocal()) {
            return;
        }
        if (command instanceof TxPrepareCommand) {
            TxPrepareCommand prepare = (TxPrepareCommand) command;
            for (Member peer : WrenchConfiguration.getConfiguration().getPeers()) {
                try {
                    communicator.notifyPrepare(prepare.getTransactionId(), peer);
                    log.info("Notified peer: " + peer.getProcessId() + " about PREPARE " +
                            prepare.getTransactionId());
                    break;
                } catch (WrenchException e) {
                    log.error("Error while contacting remote peer", e);
                }
            }
        }
    }


}
