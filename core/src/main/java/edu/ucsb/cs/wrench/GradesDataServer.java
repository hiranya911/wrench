package edu.ucsb.cs.wrench;

import edu.ucsb.cs.wrench.commands.Command;
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

    }


}
