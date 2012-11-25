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
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PaxosAgent {

    private static final Log log = LogFactory.getLog(PaxosAgent.class);

    private static final int COLD_START             = 0;
    private static final int LEADER_DISCOVERY       = 1;
    private static final int LEADER_ELECTION        = 2;
    private static final int LEADER_RECOVERY_MODE   = 3;
    private static final int LEADER_NORMAL_MODE     = 4;
    private static final int COHORT                 = 5;

    private PaxosLedger ledger;
    private Member leader;
    private TServer server;

    private ExecutorService exec = Executors.newFixedThreadPool(5);
    private WrenchCommunicator communicator = new WrenchCommunicator();
    private WrenchConfiguration config = WrenchConfiguration.getConfiguration();
    private ElectionWorker electionWorker = new ElectionWorker(communicator);
    private int status = COLD_START;
    private AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private Set<AckEvent> ackStore = new HashSet<AckEvent>();
    private Map<Long,AtomicInteger> acceptCount = new HashMap<Long, AtomicInteger>();
    private Map<Long,Command> commands = new HashMap<Long, Command>();

    private Future thriftFuture;
    private Future paxosFuture;

    private final Object lock = new Object();

    private Queue<PaxosEvent> eventQueue = new ConcurrentLinkedQueue<PaxosEvent>();

    public void start() {
        try {
            ledger = new PaxosLedger();
        } catch (IOException e) {
            handleException("Error while initializing Paxos ledger", e);
        }

        final WrenchManagementServiceHandler handler = new WrenchManagementServiceHandler(this);
        WrenchManagementService.Processor<WrenchManagementServiceHandler> processor =
                new WrenchManagementService.Processor<WrenchManagementServiceHandler>(handler);
        int port = config.getServerPort();

        try {
            TServerTransport serverTransport = new TServerSocket(port);
            server = new TThreadPoolServer(new TThreadPoolServer.
                    Args(serverTransport).processor(processor));
            thriftFuture = exec.submit(new Runnable() {
                @Override
                public void run() {
                    server.serve();
                }
            });
            paxosFuture = exec.submit(new PaxosWorker());
            log.info("Wrench server initialized on port: " + port);
        } catch (TTransportException e) {
            handleException("Error while starting Wrench management service", e);
        }

        status = LEADER_DISCOVERY;
        Member existingLeader = electionWorker.discoverLeader();
        if (existingLeader != null) {
            leader = existingLeader;
        } else {
            onElection();
            synchronized (lock) {
                while (leader == null) {
                    log.info("Waiting for leader election to finish");
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        log.error("Unexpected interrupt", e);
                    }
                }
            }
        }

        if (leader.isLocal()) {
            startPaxosPreparationPhase();
        } else {
            status = COHORT;
        }
    }

    public void stop() {
        thriftFuture.cancel(true);
        paxosFuture.cancel(true);
        exec.shutdownNow();
    }

    public void enqueue(PaxosEvent event) {
        eventQueue.offer(event);
    }

    public void onPrepare(PrepareEvent prepare) {
        BallotNumber ballotNumber = prepare.getBallotNumber();
        long requestNumber = prepare.getRequestNumber();
        BallotNumber nextBallotNumber = ledger.getNextBallotNumber(requestNumber);
        if (ballotNumber.compareTo(nextBallotNumber) > 0) {
            log.info("Received new and higher ballot number: " + ballotNumber);
            ledger.logNextBallotNumber(requestNumber, ballotNumber);
            AckEvent ack = new AckEvent(ballotNumber, ledger.getPreviousBallotNumbers(requestNumber),
                    ledger.getPreviousCommands(requestNumber),
                    ledger.getPreviousOutcomes(requestNumber));
            if (ballotNumber.getProcessId().equals(config.getLocalMember().getProcessId())) {
                onAck(ack);
            } else {
                communicator.sendAck(ack, WrenchConfiguration.getConfiguration().getMember(
                        ballotNumber.getProcessId()));
            }
            log.info("ACK sent to: " + ballotNumber.getProcessId());
        } else {
            log.info("Received smaller ballot number: " + ballotNumber + " - Ignoring");
        }
    }

    public synchronized void onAck(AckEvent ack) {
        if (status != LEADER_RECOVERY_MODE || !leader.isLocal()) {
            log.info("Delayed or invalid ack - Ignoring");
        } else {
            ackStore.add(ack);
            if (ackStore.size() > config.getMembers().length/2.0) {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }
    }

    public void onAccept(AcceptEvent accept) {
        BallotNumber ballotNumber = accept.getBallotNumber();
        long requestNumber = accept.getRequestNumber();
        Command command = accept.getCommand();
        if (ballotNumber.compareTo(ledger.getNextBallotNumber(requestNumber)) == 0) {
            log.info("Accepting proposal with ballot number: " + ballotNumber);
            log.info("Accepted value: " + command);
            ledger.logAcceptance(requestNumber, ballotNumber, command);
            communicator.sendAccepted(ballotNumber, requestNumber,
                    config.getMember(ballotNumber.getProcessId()));
            log.info("Sent back ACCEPTED for: " + requestNumber);
        }
    }

    public synchronized void onAccepted(AcceptedEvent accepted) {
        BallotNumber ballotNumber = accepted.getBallotNumber();
        long requestNumber = accepted.getRequestNumber();
        if (ballotNumber.compareTo(ledger.getLastTriedBallotNumber()) == 0 &&
                acceptCount.containsKey(requestNumber)) {
            int count = acceptCount.get(requestNumber).incrementAndGet();
            if (count > config.getMembers().length / 2.0) {
                log.info("Received ACCEPTED for: " + requestNumber + " from majority");
                onDecide(new DecideEvent(ballotNumber, requestNumber,
                        commands.get(requestNumber)), true);
                commands.remove(requestNumber);
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }
    }

    public void onDecide(DecideEvent decide, boolean advertise) {
        long requestNumber = decide.getRequestNumber();
        Command command = decide.getCommand();
        log.info("Deciding the outcome of " + requestNumber + " as: " + command);
        ledger.logOutcome(requestNumber, command);
        if (leader.isLocal() && advertise) {
            communicator.sendDecide(requestNumber, command);
            log.info("Sent DECIDE for: " + requestNumber);
        }
    }

    public boolean onLeaderQuery() {
        return leader != null && leader.isLocal();
    }

    public void onElection() {
        if (electionInProgress.compareAndSet(false, true)) {
            status = LEADER_ELECTION;
            exec.submit(electionWorker);
        } else {
            log.info("Election already in progress - Not starting another one");
        }
    }

    public void onVictory(Member member) {
        if (electionInProgress.get() && !member.isLocal()) {
            electionWorker.setVictoryMessageReceived(true);
        }

        if (leader == null) {
            log.info("New leader elected: " + member.getProcessId());
            leader = member;
            synchronized (lock) {
                lock.notifyAll();
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

    private void startPaxosPreparationPhase() {
        status = LEADER_RECOVERY_MODE;
        while (true) {
            log.info("Starting Paxos preparation phase");
            ackStore.clear();
            long lastExecuted = ledger.getLastExecutedRequest();
            if (lastExecuted == -1) {
                lastExecuted = 1;
            } else {
                lastExecuted++;
            }

            BallotNumber ballotNumber = ledger.getLastTriedBallotNumber();
            if (ballotNumber == null) {
                ballotNumber = new BallotNumber(0, config.getLocalMember().getProcessId());
            } else {
                ballotNumber.increment();
            }
            ledger.logLastTriedBallotNumber(ballotNumber);
            communicator.sendPrepare(ballotNumber, lastExecuted);

            synchronized (lock) {
                try {
                    lock.wait(30000);
                } catch (InterruptedException ignored) {
                }
            }

            if (ackStore.size() > config.getMembers().length/2.0) {
                log.info("Received majority ack messages");
                finishPaxosPreparationPhase(ballotNumber);
                return;
            } else {
                log.info("Failed to obtain majority ack messages - Retrying in 2 seconds");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private void finishPaxosPreparationPhase(BallotNumber ballotNumber) {
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
                onDecide(new DecideEvent(ballotNumber, entry.getKey(), entry.getValue()), false);
            }
        }

        if (pendingCommands.size() > 0) {
            for (Map.Entry<Long,Command> entry : pendingCommands.entrySet()) {
                log.info("Running accept phase on: " + entry.getKey());
                sendAccept(ballotNumber, entry.getKey(), entry.getValue());
            }
        }

        status = LEADER_NORMAL_MODE;
    }

    private void sendAccept(BallotNumber ballotNumber, Long requestNumber, Command command) {
        acceptCount.put(requestNumber, new AtomicInteger(0));
        commands.put(requestNumber, command);
        communicator.sendAccept(ballotNumber, requestNumber, command);
        synchronized (lock) {
            try {
                lock.wait(30000);
            } catch (InterruptedException ignored) {
            }
        }

        if (ledger.getOutcome(requestNumber).equals(command)) {
            log.info("Request number " + requestNumber + " is fully dealt with");
        } else {
            log.warn("Failed to obtain majority consensus");
        }
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new WrenchException(msg, e);
    }

    private class PaxosWorker implements Runnable {

        @Override
        public void run() {
            while (true) {
                PaxosEvent event = eventQueue.poll();
                if (event == null) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
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
                    onDecide((DecideEvent) event, true);
                }
            }
        }
    }
}
