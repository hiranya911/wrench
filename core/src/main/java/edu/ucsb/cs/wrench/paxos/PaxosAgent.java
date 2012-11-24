package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.WrenchException;
import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.messaging.WrenchCommunicator;
import edu.ucsb.cs.wrench.messaging.WrenchManagementService;
import edu.ucsb.cs.wrench.messaging.WrenchManagementServiceHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class PaxosAgent {

    private static final Log log = LogFactory.getLog(PaxosAgent.class);

    private PaxosLedger ledger;
    private ExecutorService exec;
    private Member leader;
    private TServer server;
    private WrenchCommunicator communicator;

    private ElectionWorker electionWorker;
    private AtomicBoolean electionInProgress;


    public void start() {
        try {
            ledger = new PaxosLedger();
        } catch (IOException e) {
            handleException("Error while initializing Paxos ledger", e);
        }
        exec = Executors.newFixedThreadPool(5);
        communicator = new WrenchCommunicator();
        electionWorker = new ElectionWorker(communicator);
        electionInProgress = new AtomicBoolean(false);

        final WrenchManagementServiceHandler handler = new WrenchManagementServiceHandler(this);
        WrenchManagementService.Processor<WrenchManagementServiceHandler> processor =
                new WrenchManagementService.Processor<WrenchManagementServiceHandler>(handler);
        int port = WrenchConfiguration.getConfiguration().getServerPort();

        try {
            TServerTransport serverTransport = new TServerSocket(port);
            server = new TThreadPoolServer(new TThreadPoolServer.
                    Args(serverTransport).processor(processor));
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    server.serve();
                }
            });
            log.info("Wrench server initialized on port: " + port);
        } catch (TTransportException e) {
            handleException("Error while starting Wrench management service", e);
        }

        onElection();
        synchronized (this) {
            while (leader == null) {
                try {
                    log.info("Waiting for leader election to finish");
                    this.wait(1000);
                } catch (InterruptedException e) {
                    log.error("Unexpected interrupt", e);
                }
            }
        }
    }

    public void onPrepare(BallotNumber ballotNumber) {

    }

    public void onAccept(BallotNumber ballotNumber, long requestNumber, Command command) {

    }

    public void onDecide(long requestNumber, Command command) {

    }

    public void onElection() {
        if (electionInProgress.compareAndSet(false, true)) {
            exec.submit(electionWorker);
        } else {
            log.info("Election already in progress - Not starting another one");
        }
    }

    public void onVictory(Member member) {
        if (!member.isLocal()) {
            electionWorker.setVictoryMessageReceived(true);
        }

        if (leader == null) {
            log.info("New leader elected: " + member.getProcessId());
            leader = member;
            synchronized (this) {
                this.notifyAll();
            }
        } else {
            if (leader.equals(member)) {
                log.info("Election came to an end with the current leader extending his lease");
            } else {
                log.info("Change of leadership: " + leader.getProcessId() +
                        " --> " + member.getProcessId());
                leader = member;
            }
        }
        electionInProgress.compareAndSet(true, false);
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new WrenchException(msg, e);
    }
}
