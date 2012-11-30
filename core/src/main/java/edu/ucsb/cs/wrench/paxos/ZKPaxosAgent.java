package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.WrenchException;
import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.NullCommand;
import edu.ucsb.cs.wrench.commands.TxCommitCommand;
import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.messaging.WrenchCommunicator;
import edu.ucsb.cs.wrench.messaging.WrenchManagementService;
import edu.ucsb.cs.wrench.messaging.WrenchManagementServiceHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ZKPaxosAgent implements Watcher, AsyncCallback.ChildrenCallback, AgentService {

    protected final Log log = LogFactory.getLog(this.getClass());

    private static final String ELECTION_NODE = "/ELECTION";

    private ExecutorService zkService = Executors.newSingleThreadExecutor();
    private ZooKeeper zkClient;
    private WrenchConfiguration config = WrenchConfiguration.getConfiguration();
    protected Member leader;
    private PaxosLedger ledger;
    private TServer server;
    protected WrenchCommunicator communicator = new WrenchCommunicator();
    private Set<AckEvent> ackStore = new HashSet<AckEvent>();
    private Map<Long,AtomicInteger> acceptCount = new ConcurrentHashMap<Long, AtomicInteger>();
    private Future paxosTask;
    protected ExecutorService exec = Executors.newCachedThreadPool();
    private final Object prepareLock = new Object();
    private final Object stateMachineLock = new Object();
    private final Queue<PaxosEvent> eventQueue = new ConcurrentLinkedQueue<PaxosEvent>();
    private Map<Long,Command> commands = new ConcurrentHashMap<Long, Command>();
    private PaxosWorker paxosWorker = new PaxosWorker();
    private PaxosProtocolState protocolState = PaxosProtocolState.IDLE;
    private PaxosAgentMode agentMode = PaxosAgentMode.UNDEFINED;
    private final AtomicLong nextRequest = new AtomicLong(1);
    private AtomicLong nextLineNumber = new AtomicLong(1);
    private boolean acceptRequests = false;
    protected final Set<String> pendingTransactions = new HashSet<String>();

    public void start() {
        Properties properties = new Properties();
        String configPath = System.getProperty("wrench.config.dir", "conf");
        String dataPath = System.getProperty("wrench.zk.dir", "zk");
        File configFile = new File(configPath, "zk.properties");
        try {
            properties.load(new FileInputStream(configFile));
            properties.setProperty("dataDir", dataPath);
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(properties);
            ZKServer server = new ZKServer(config);
            zkService.submit(server);
            log.info("ZooKeeper server started");
        } catch (IOException e) {
            handleException("Error loading the ZooKeeper configuration", e);
        } catch (QuorumPeerConfig.ConfigException e) {
            handleException("Error loading the ZooKeeper configuration", e);
        }

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

        // Execute any commands that I agreed to execute earlier but didn't get
        // an opportunity to finish.
        RequestHistory history = ledger.getRequestHistory(ledger.getLastExecutedRequest());
        Map<Long,Command> pastCommands = history.getPrevOutcomes();
        if (pastCommands.size() > 0) {
            log.info("Executing " + pastCommands.size() + " past commands from the ledger");
            for (Map.Entry<Long,Command> entry : pastCommands.entrySet()) {
                executeCommands(entry.getKey(), entry.getValue());
            }
        }

        // Start the Paxos event processor
        paxosTask = exec.submit(paxosWorker);

        String connection = "localhost:" + properties.getProperty("clientPort");
        try {
            zkClient = new ZooKeeper(connection, 5000, this);
        } catch (IOException e) {
            handleException("Error initializing the ZooKeeper client", e);
        }
    }

    public void stop() {
        server.stop();
        System.out.println("Stopped the Thrift service");
        paxosWorker.stop();
        synchronized (stateMachineLock) {
            stateMachineLock.notifyAll();
        }
        while (paxosTask != null && !paxosTask.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }
        System.out.println("Stopped Paxos task");
        exec.shutdownNow();
        zkService.shutdownNow();
        System.out.println("Program terminated");
    }

    public abstract void onDecision(long requestNumber, Command command);

    public void enqueue(PaxosEvent event) {
        synchronized (eventQueue) {
            eventQueue.offer(event);
            eventQueue.notifyAll();
        }
    }

    public boolean executeClientRequest(final Command command) {
        while (true) {
            synchronized (stateMachineLock) {
                while (agentMode != PaxosAgentMode.COHORT && agentMode != PaxosAgentMode.LEADER) {
                    try {
                        stateMachineLock.wait(5000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            if (!acceptRequests) {
                log.info("Not accepting requests at the moment");
                return false;
            }

            final long requestNumber;
            synchronized (nextRequest) {
                requestNumber = nextRequest.getAndIncrement();
                if (command instanceof TxCommitCommand) {
                    TxCommitCommand commit = (TxCommitCommand) command;
                    if (commit.getLineNumber() == -1) {
                        commit.setLineNumber(nextLineNumber.getAndIncrement());
                    }
                }
            }
            final AtomicBoolean done = new AtomicBoolean(false);
            if (leader.isLocal()) {
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        runAcceptPhase(requestNumber, command);
                        done.compareAndSet(false, true);
                    }
                });
                log.info("Submitted request number: " + requestNumber);

                while (!done.get()) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                    }
                }

                return true;
            } else {
                log.info("Relaying the request to: " + leader.getProcessId());
                try {
                    return communicator.relayCommand(command, leader);
                } catch (WrenchException e) {
                    log.error("Failed to relay the command to leader - Retrying", e);
                }
            }
        }
    }

    private void onPrepare(PrepareEvent prepare) {
        BallotNumber ballotNumber = prepare.getBallotNumber();
        long requestNumber = prepare.getRequestNumber();
        if (ballotNumber.compareTo(ledger.getNextBallotNumber()) >= 0) {
            log.info("Received PREPARE with new and higher ballot number: " + ballotNumber);
            ledger.logNextBallotNumber(ballotNumber);
            RequestHistory history = ledger.getRequestHistory(requestNumber);
            AckEvent ack = new AckEvent(ballotNumber, history);
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
        if (ballotNumber.equals(ledger.getNextBallotNumber()) &&
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
        long requestNumber = accepted.getRequestNumber();
        AtomicInteger votes = acceptCount.get(requestNumber);
        if (votes != null) {
            synchronized (votes) {
                BallotNumber ballotNumber = accepted.getBallotNumber();
                if (ballotNumber.equals(ledger.getLastTriedBallotNumber()) &&
                        protocolState == PaxosProtocolState.POLLING) {
                    log.info("Received ACCEPTED for request: " + requestNumber);
                    if (config.isMajority(votes.incrementAndGet())) {
                        votes.notifyAll();
                    }
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
            onDecision(requestNumber, command);
            executeCommands(requestNumber, command);
        }
    }

    private synchronized void executeCommands(long requestNumber, Command command) {
        commands.put(requestNumber, command);
        while (true) {
            long next = ledger.getLastExecutedRequest() + 1;
            Command cmd = commands.get(next);
            if (cmd != null) {
                if (cmd.execute()) {
                    log.info("Executed request: " + next + ", command: " + cmd);
                    ledger.logExecution(next);
                    commands.remove(next);
                } else {
                    log.fatal("Failed to execute command: " + cmd + " - Retrying");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                }
            } else {
                break;
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            handleNoneEvent(event);
        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
            zkClient.getChildren(ELECTION_NODE, true, this, null);
        }
    }

    @Override
    public void processResult(int code, String s, Object o, List<String> children) {
        if (code != KeeperException.Code.OK.intValue()) {
            log.error("Unexpected response code from ZooKeeper server");
            return;
        }

        setAgentMode(PaxosAgentMode.VIEW_CHANGE);

        long smallest = Long.MAX_VALUE;
        String processId = null;
        for (String child : children) {
            int index = child.lastIndexOf('_');
            long sequenceNumber = Long.parseLong(child.substring(index + 1));
            if (sequenceNumber < smallest) {
                smallest = sequenceNumber;
                processId = child.substring(0, index);
            }
        }

        if (processId != null) {
            if (leader == null || !leader.getProcessId().equals(processId)) {
                boolean init = leader == null;
                log.info("Elected leader: " + processId);
                leader = config.getMember(processId);
                if (leader.isLocal()) {
                    runPreparePhase();
                } else if (init) {
                    runCohortRecoveryProcedure();
                } else {
                    setAgentMode(PaxosAgentMode.COHORT);
                }
            } else if (leader.isLocal()) {
                setAgentMode(PaxosAgentMode.LEADER);
            } else {
                setAgentMode(PaxosAgentMode.COHORT);
            }
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
                BallotNumber largestSeen = ledger.getNextBallotNumber();

                if (ballotNumber.compareTo(largestSeen) < 0) {
                    ballotNumber = new BallotNumber(largestSeen.getNumber(),
                            config.getLocalMember().getProcessId());
                }
                ballotNumber.increment();

                ledger.logLastTriedBallotNumber(ballotNumber);
                communicator.sendPrepare(ballotNumber, ledger.getLastExecutedRequest());
                try {
                    prepareLock.wait(5000);
                } catch (InterruptedException ignored) {
                }

                if (config.isMajority(ackStore.size())) {
                    log.info("Received ACK messages from a majority");
                    protocolState = PaxosProtocolState.POLLING;
                    runLeaderRecoveryProcedure();
                    return;
                } else {
                    log.info("Failed to obtain ACK messages from a majority - Retrying");
                }
            }
        }
    }

    private void runLeaderRecoveryProcedure() {
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
                pendingCommands.remove(entry.getKey());
                onDecide(new DecideEvent(entry.getKey(), entry.getValue()));
            }
        }

        long lastExecuted = ledger.getLastExecutedRequest();
        long largestDecided = ledger.getLargestDecidedRequest();
        long largestAccepted = -1;
        if (pendingCommands.size() > 0) {
            largestAccepted = Collections.max(pendingCommands.keySet());
        }
        long upperBound = Math.max(largestAccepted, largestDecided);
        for (long i = lastExecuted + 1; i <= upperBound; i++) {
            if (!pendingCommands.containsKey(i) && ledger.getOutcome(i) == null) {
                log.info("Filling out request " + i + " with the NULL command");
                pendingCommands.put(i, new NullCommand());
            }
        }

        if (pendingCommands.size() > 0) {
            for (Map.Entry<Long,Command> entry : pendingCommands.entrySet()) {
                log.info("Running accept phase on: " + entry.getKey());
                runAcceptPhase(entry.getKey(), entry.getValue());
            }
        }

        log.info("Successfully finished the Paxos recovery (view change) mode");
        long last = ledger.getLargestDecidedRequest();
        if (last < 0) {
            nextRequest.set(1);
        }  else {
            nextRequest.set(last + 1);
        }

        try {
            File dbDir = new File(config.getWrenchHome(), config.getDBDirectoryPath());
            File dataFile = new File(dbDir, config.getDataFileName());
            List<String> lines = FileUtils.readLines(dataFile);
            nextLineNumber.set(lines.size() + 1);
        } catch (FileNotFoundException e) {
            nextLineNumber.set(1);
        } catch (IOException e) {
            handleFatalException("Unrecoverable error while reading from file system", e);
        }
        setAgentMode(PaxosAgentMode.LEADER);
    }

    private void runCohortRecoveryProcedure() {
        while (true) {
            try {
                BallotNumber ballotNumber = communicator.getNextBallotNumber(leader);
                Map<Long,Command> pastCommands = communicator.getPastOutcomes(
                        ledger.getLastExecutedRequest(), leader);

                if (ballotNumber != null && pastCommands != null) {
                    ledger.logNextBallotNumber(ballotNumber);
                    if (pastCommands.size() > 0) {
                        log.info("Executing " + pastCommands.size() +
                                " past commands borrowed from the leader");
                        for (Map.Entry<Long,Command> entry : pastCommands.entrySet()) {
                            onDecide(new DecideEvent(entry.getKey(), entry.getValue()));
                        }
                    }
                }
                setAgentMode(PaxosAgentMode.COHORT);
                break;
            } catch (WrenchException e) {
                log.warn("Unable to reach the leader to obtain Paxos metadata", e);
            }
        }
    }

    private void runAcceptPhase(long requestNumber, Command command) {
        if (protocolState != PaxosProtocolState.POLLING) {
            throw new WrenchException("Invalid protocol state. Expected: " +
                    PaxosProtocolState.POLLING + ", Found: " + protocolState);
        }

        AtomicInteger votes = new AtomicInteger(0);
        acceptCount.put(requestNumber, votes);
        BallotNumber lastTried = ledger.getLastTriedBallotNumber();
        communicator.sendAccept(lastTried, requestNumber, command);
        synchronized (votes) {
            try {
                votes.wait(5000);
            } catch (InterruptedException ignored) {
            }
        }

        acceptCount.remove(requestNumber);
        if (config.isMajority(votes.get())) {
            log.info("Request number " + requestNumber + " is fully dealt with");
            communicator.sendDecide(lastTried, requestNumber, command);
            onDecide(new DecideEvent(lastTried, requestNumber, command));
        } else {
            handleFatalException("Failed to obtain majority consensus", null);
        }
    }

    public void onAppendNotification(String transactionId) {
        if (leader.isLocal()) {
            synchronized (pendingTransactions) {
                pendingTransactions.add(transactionId);
                pendingTransactions.notifyAll();
            }
        } else {
            communicator.notifyPrepare(transactionId, leader);
        }
    }

    public boolean onAppendCommit(String transactionId, long lineNumber) {
        if (leader.isLocal()) {
            TxCommitCommand commit = new TxCommitCommand(transactionId);
            commit.setLineNumber(lineNumber);
            return executeClientRequest(new TxCommitCommand(transactionId));
        } else {
            return communicator.notifyCommit(transactionId, lineNumber, leader);
        }
    }

    private void handleNoneEvent(WatchedEvent event) {
        switch (event.getState()) {
            case SyncConnected:
                log.info("Successfully connected to the ZooKeeper server");
                createElectionRoot();
                zkClient.getChildren(ELECTION_NODE, true, this, null);
                String zNode = ELECTION_NODE + "/" + config.getLocalMember().getProcessId() + "_";
                try {
                    zkClient.create(zNode, new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                } catch (Exception e) {
                    handleFatalException("Failed to create z_node", e);
                }
                acceptRequests = true;
                break;
            case Disconnected:
                log.warn("Lost the connection to ZooKeeper server");
                acceptRequests = false;
                break;
            case Expired:
                handleFatalException("Connection to ZooKeeper server expired", null);
        }
    }

    private void createElectionRoot() {
        try {
            Stat stat = zkClient.exists(ELECTION_NODE, false);
            if (stat == null) {
                try {
                    zkClient.create(ELECTION_NODE, new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException ignored) {
                }
            }
        } catch (Exception e) {
            handleFatalException("Error while creating the ELECTION root", e);
        }
    }

    private void setAgentMode(PaxosAgentMode mode) {
        synchronized (stateMachineLock) {
            agentMode = mode;
            stateMachineLock.notifyAll();
        }
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new WrenchException(msg, e);
    }

    private void handleFatalException(String msg, Exception e) {
        if (e != null) {
            log.fatal(msg, e);
        } else {
            log.fatal(msg);
        }
        System.exit(1);
    }

    private class ZKServer implements Runnable {

        private QuorumPeerConfig config;
        private QuorumPeerMain peer;

        private ZKServer(QuorumPeerConfig config) {
            this.config = config;
            this.peer = new QuorumPeerMain();
        }

        @Override
        public void run() {
            try {
                peer.runFromConfig(config);
            } catch (IOException e) {
                handleFatalException("Fatal error encountered in ZooKeeper server", e);
            }
        }
    }

    private class PaxosWorker implements Runnable {

        boolean stop = false;

        @Override
        public void run() {
            log.info("Initializing Paxos task");
            while (!stop || !eventQueue.isEmpty()) {
                PaxosEvent event;
                synchronized (eventQueue) {
                    event = eventQueue.poll();
                    if (event == null) {
                        try {
                            eventQueue.wait();
                        } catch (InterruptedException ignored) {
                        }
                        continue;
                    }
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

    @Override
    public void onElection() {

    }

    @Override
    public void onVictory(Member member) {

    }

    @Override
    public boolean onLeaderQuery() {
        return leader != null && leader.isLocal();
    }

    public Map<Long,Command> getPastOutcomes(long requestNumber) {
        RequestHistory history = ledger.getRequestHistory(requestNumber);
        return history.getPrevOutcomes();
    }

    public BallotNumber getNextBallotNumber() {
        return ledger.getNextBallotNumber();
    }
}
