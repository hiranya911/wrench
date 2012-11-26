package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.WrenchException;
import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.elections.ElectionCommissioner;
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
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PaxosAgent {

    private static final Log log = LogFactory.getLog(PaxosAgent.class);

    private PaxosLedger ledger;
    private Member leader;
    private TServer server;

    private ExecutorService exec = Executors.newFixedThreadPool(5);
    private WrenchCommunicator communicator = new WrenchCommunicator();
    private ElectionCommissioner electionCommissioner = new ElectionCommissioner(communicator);
    private WrenchConfiguration config = WrenchConfiguration.getConfiguration();
    private AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private Set<AckEvent> ackStore = new HashSet<AckEvent>();
    private Map<Long,AtomicInteger> acceptCount = new HashMap<Long, AtomicInteger>();

    private Future thriftTask;
    private Future paxosTask;

    private final Object prepareLock = new Object();
    private final Object acceptLock = new Object();
    private final Object electionLock = new Object();

    private final Queue<PaxosEvent> eventQueue = new ConcurrentLinkedQueue<PaxosEvent>();

    private PaxosWorker paxosWorker = new PaxosWorker();
    private PaxosProtocolState protocolState = PaxosProtocolState.IDLE;

    public void start() {
        try {
            ledger = new PaxosLedger();
        } catch (IOException e) {
            handleException("Error while initializing Paxos ledger", e);
        }

        WrenchManagementServiceHandler handler = new WrenchManagementServiceHandler(this);
        WrenchManagementService.Processor<WrenchManagementServiceHandler> processor =
                new WrenchManagementService.Processor<WrenchManagementServiceHandler>(handler);
        int port = config.getServerPort();
        try {
            TServerTransport serverTransport = new TServerSocket(port);
            server = new TThreadPoolServer(new TThreadPoolServer.
                    Args(serverTransport).processor(processor));
            thriftTask = exec.submit(new Runnable() {
                @Override
                public void run() {
                    server.serve();
                }
            });
            log.info("Wrench server initialized on port: " + port);
        } catch (TTransportException e) {
            handleException("Error while starting Wrench management service", e);
        }

        synchronized (electionLock) {
            leader = electionCommissioner.discoverLeader();
            if (leader != null) {
                log.info("Discovered existing leader: " + leader.getProcessId());
            } else {
                startElection();
                while (leader == null) {
                    try {
                        electionLock.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        paxosTask = exec.submit(paxosWorker);
        if (leader.isLocal()) {
            runPreparePhase();
        }
    }

    public void enqueue(PaxosEvent event) {
        synchronized (eventQueue) {
            eventQueue.offer(event);
            eventQueue.notifyAll();
        }
    }

    public void stop() {
        thriftTask.cancel(true);
        System.out.println("Stopped the Thrift service");
        paxosWorker.stop();
        while (paxosTask != null && !paxosTask.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }
        System.out.println("Stopped Paxos task");
        exec.shutdownNow();
        System.out.println("Program terminated");
    }

    private void onPrepare(PrepareEvent prepare) {
        BallotNumber ballotNumber = prepare.getBallotNumber();
        long requestNumber = prepare.getRequestNumber();
        if (ballotNumber.compareTo(ledger.getNextBallotNumber(requestNumber)) >= 0) {
            log.info("Received PREPARE with new and higher ballot number: " + ballotNumber);
            ledger.logNextBallotNumber(requestNumber, ballotNumber);
            AckEvent ack = new AckEvent(ballotNumber,
                    ledger.getPreviousBallotNumbers(requestNumber),
                    ledger.getPreviousCommands(requestNumber),
                    ledger.getPreviousOutcomes(requestNumber));
            Member sender = config.getMember(ballotNumber.getProcessId());
            log.info("Sending ACK to: " + ballotNumber.getProcessId());
            if (sender.isLocal()) {
                onAck(ack);
            } else {
                communicator.sendAck(ack, sender);
            }
        } else {
            log.info("Received PREPARE with smaller ballot number: " + ballotNumber + " - Ignoring");
        }
    }

    private void onAck(AckEvent ack) {
        synchronized (prepareLock) {
            if (ack.getBallotNumber().equals(ledger.getLastTriedBallotNumber()) &&
                    protocolState == PaxosProtocolState.TRYING) {
                log.info("Received ACK with ballot number: " + ack.getBallotNumber());
                ackStore.add(ack);
                if (config.isMajority(ackStore.size())) {
                    prepareLock.notifyAll();
                }
            }
        }
    }

    private void onAccept(AcceptEvent accept) {
        BallotNumber ballotNumber = accept.getBallotNumber();
        long requestNumber = accept.getRequestNumber();
        Command command = accept.getCommand();
        if (ballotNumber.equals(ledger.getNextBallotNumber(requestNumber)) &&
                ballotNumber.compareTo(ledger.getPreviousBallotNumber(requestNumber)) > 0) {
            log.info("Received ACCEPT with the expected ballot: " + ballotNumber);
            ledger.logAcceptance(requestNumber, ballotNumber, command);
            log.info("Accepted request " + requestNumber + " with value: " + command);
            Member sender = config.getMember(ballotNumber.getProcessId());
            log.info("Sending ACCEPTED to: " + sender.getProcessId());
            AcceptedEvent accepted = new AcceptedEvent(ballotNumber, requestNumber);
            if (sender.isLocal()) {
                onAccepted(accepted);
            } else {
                communicator.sendAccepted(accepted, sender);
            }
        } else {
            log.info("Received ACCEPT with unexpected ballot number: " + ballotNumber + " - Ignoring");
        }
    }

    private void onAccepted(AcceptedEvent accepted) {
        synchronized (acceptLock) {
            BallotNumber ballotNumber = accepted.getBallotNumber();
            long requestNumber = accepted.getRequestNumber();
            if (ballotNumber.equals(ledger.getLastTriedBallotNumber()) &&
                    protocolState == PaxosProtocolState.POLLING) {
                log.info("Received ACCEPTED for request: " + requestNumber);
                int votes = acceptCount.get(requestNumber).incrementAndGet();
                if (config.isMajority(votes)) {
                    acceptLock.notifyAll();
                }
            }
        }
    }

    private void onDecide(DecideEvent decide) {
        long requestNumber = decide.getRequestNumber();
        Command command = decide.getCommand();
        if (ledger.getOutcome(requestNumber) == null) {
            log.info("Received DECIDE with request number: " + requestNumber +
                    " and command: " + command);
            ledger.logOutcome(requestNumber, command);
        }
    }

    private void runPreparePhase() {
        while (true) {
            synchronized (prepareLock) {
                log.info("Starting Paxos prepare phase");
                protocolState = PaxosProtocolState.TRYING;
                ackStore.clear();
                BallotNumber ballotNumber = ledger.getLastTriedBallotNumber();
                if (ballotNumber == null) {
                    ballotNumber = new BallotNumber(0, config.getLocalMember().getProcessId());
                }
                BallotNumber largestSeen = ledger.getLargestPreviousBallotNumber();

                if (ballotNumber.compareTo(largestSeen) < 0) {
                    ballotNumber = new BallotNumber(largestSeen.getNumber(),
                            config.getLocalMember().getProcessId());
                }
                ballotNumber.increment();

                long requestNumber = ledger.getLastExecutedRequest();
                if (requestNumber == -1) {
                    requestNumber = 1;
                } else {
                    requestNumber++;
                }

                ledger.logLastTriedBallotNumber(ballotNumber);
                communicator.sendPrepare(ballotNumber, requestNumber);
                try {
                    prepareLock.wait(5000);
                } catch (InterruptedException ignored) {
                }

                if (config.isMajority(ackStore.size())) {
                    log.info("Received ACK messages from a majority");
                    protocolState = PaxosProtocolState.POLLING;
                    runRecoveryProcedure(ballotNumber);
                    return;
                } else {
                    log.info("Failed to obtain ACK messages from a majority - Retrying");
                }
            }
        }
    }

    private void runRecoveryProcedure(BallotNumber ballotNumber) {
        Map<Long,Command> pastOutcomes = new HashMap<Long, Command>();
        Map<Long,BallotNumber> largestBallot = new HashMap<Long, BallotNumber>();
        Map<Long,Command> pendingCommands = new HashMap<Long, Command>();
        for (AckEvent ack : ackStore) {
            pastOutcomes.putAll(ack.getPastOutcomes());
            for (Map.Entry<Long,BallotNumber> acceptNum : ack.getAcceptNumbers().entrySet()) {
                BallotNumber largestAcceptNum = largestBallot.get(acceptNum.getKey());
                if (largestAcceptNum == null || largestAcceptNum.compareTo(acceptNum.getValue()) < 0) {
                    largestBallot.put(acceptNum.getKey(), acceptNum.getValue());
                    pendingCommands.put(acceptNum.getKey(),
                            ack.getAcceptValues().get(acceptNum.getKey()));
                }
            }
        }

        if (pastOutcomes.size() > 0) {
            for (Map.Entry<Long,Command> entry : pastOutcomes.entrySet()) {
                log.info("Merging the outcome of request: " + entry.getKey() + " to the local store");
                onDecide(new DecideEvent(ballotNumber, entry.getKey(), entry.getValue()));
            }
        }

        if (pendingCommands.size() > 0) {
            for (Map.Entry<Long,Command> entry : pendingCommands.entrySet()) {
                log.info("Running accept phase on: " + entry.getKey());
                runAcceptPhase(ballotNumber, entry.getKey(), entry.getValue());
            }
        }

        log.info("Successfully finished the Paxos recovery (view change) mode");
    }

    private void runAcceptPhase(BallotNumber ballotNumber, long requestNumber, Command command) {
        synchronized (acceptLock) {
            if (protocolState != PaxosProtocolState.POLLING) {
                throw new WrenchException("Invalid protocol state. Expected: " +
                        PaxosProtocolState.POLLING + ", Found: " + protocolState);
            }
            acceptCount.put(requestNumber, new AtomicInteger(0));
            communicator.sendAccept(ballotNumber, requestNumber, command);
            try {
                acceptLock.wait(5000);
            } catch (InterruptedException ignored) {
            }

            int votes = acceptCount.get(requestNumber).get();
            if (config.isMajority(votes)) {
                log.info("Request number " + requestNumber + " is fully dealt with");
                ledger.logOutcome(requestNumber, command);
                communicator.sendDecide(ballotNumber, requestNumber, command);
            } else {
                log.warn("Failed to obtain majority consensus");
            }
        }
    }

    public void onElection() {
        log.info("Received ELECTION request");
        startElection();
    }

    private void startElection() {
        if (electionInProgress.compareAndSet(false, true)) {
            synchronized (electionLock) {
                leader = null;
                while (leader == null) {
                    exec.submit(electionCommissioner);
                    try {
                        electionLock.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        } else {
            log.info("Election already in progress - Not starting another");
        }
    }

    public void onVictory(Member winner) {
        synchronized (electionLock) {
            log.info("New leader elected - All hail: " + winner.getProcessId());
            leader = winner;
            if (electionInProgress.compareAndSet(true, false)) {
                electionCommissioner.setWinner(winner);
            }
            electionLock.notifyAll();
        }
    }

    public boolean onLeaderQuery() {
        synchronized (electionLock) {
            return leader != null && leader.isLocal();
        }
    }

    private class PaxosWorker implements Runnable {

        boolean stop = false;

        @Override
        public void run() {
            log.info("Initializing Paxos task");
            while (!stop || !eventQueue.isEmpty()) {
                synchronized (electionLock) {
                    while (leader == null) {
                        log.info("Pausing Paxos task until leader election is complete");
                        try {
                            electionLock.wait();
                        } catch (InterruptedException ignored) {
                        }
                        log.info("Resuming Paxos task");
                    }
                }

                PaxosEvent event = eventQueue.poll();
                if (event == null) {
                    synchronized (eventQueue) {
                        try {
                            eventQueue.wait();
                        } catch (InterruptedException ignored) {
                        }
                    }
                    continue;
                }

                if (event instanceof PrepareEvent) {
                    onPrepare((PrepareEvent) event);
                } else if (event instanceof AckEvent) {
                    onAck((AckEvent) event);
                } else if (event instanceof AcceptEvent) {
                    onAccept((AcceptEvent) event);
                } else if (event instanceof AcceptedEvent) {
                    onAccepted((AcceptedEvent) event);
                } else if (event instanceof DecideEvent) {
                    onDecide((DecideEvent) event);
                }
            }
        }

        private void stop() {
            this.stop = true;
            synchronized (eventQueue) {
                eventQueue.notifyAll();
            }
        }
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new WrenchException(msg, e);
    }

}
