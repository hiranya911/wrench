package edu.ucsb.cs.paxos;

import edu.ucsb.cs.paxos.log.CommitLog;
import edu.ucsb.cs.paxos.log.LogEntry;
import edu.ucsb.cs.paxos.log.PrepareEntry;
import edu.ucsb.cs.paxos.msg.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PaxosAgent implements Runnable {

    private int processId;
    private boolean leader;

    private BallotNumber ballotNumber = new BallotNumber(0,0);
    private AcceptNumber acceptNumber = new AcceptNumber();

    private Queue<Message> messages = new ConcurrentLinkedQueue<Message>();

    private Queue<AcceptNumber> prepareResponses =
            new ConcurrentLinkedQueue<AcceptNumber>();
    private Map<LogEntry,Integer> acceptResponses =
            new ConcurrentHashMap<LogEntry, Integer>();

    private CommitLog log = new CommitLog();

    private int posCounter = 0;

    public PaxosAgent(int processId) {
        this.processId = processId;
    }

    @Override
    public void run() {
        if (!leader && processId == 0) {
            log("Running for leadership position");
            prepare();
        }

        while (true) {
            Message msg = messages.poll();
            if (msg != null) {
                if (msg instanceof Prepare) {
                    receivePrepare((Prepare) msg);
                } else if (msg instanceof PrepareAck) {
                    receivePrepareAck((PrepareAck) msg);
                } else if (msg instanceof Accepted) {
                    receiveAccepted((Accepted) msg);
                } else if (msg instanceof Accept) {
                    receiveAccept((Accept) msg);
                } else if (msg instanceof Append) {
                    if (leader) {
                        receiveAppend((Append) msg);
                    } else {
                        log("APPEND received at non-leader agent");
                    }
                }
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {

                }
            }
        }
    }

    private void prepare() {
        ballotNumber.processId = processId;
        ballotNumber.proposalNumber += 1;
        Network.broadcast(new Prepare(this, ballotNumber));
    }

    public void deliver(Message message) {
        messages.offer(message);
    }

    private int receiveAppend(Append append) {
        LogEntry entry = new PrepareEntry(append.getTransactionId());
        int requestNumber = posCounter++;
        Network.broadcast(new Accept(this, requestNumber, ballotNumber, entry));
        return requestNumber;
    }

    private void receivePrepare(Prepare prepare) {
        log("Received PREPARE message with ballot number: " + prepare.getBallotNumber());
        if (prepare.getBallotNumber().compareTo(ballotNumber) > 0) {
            log("PREPARE message has a larger ballot number - Updating local state");
            ballotNumber = prepare.getBallotNumber();
            Network.send(prepare.getSender(),
                    new PrepareAck(this, ballotNumber, acceptNumber));
        } else {
            log("PREPARE message has a smaller ballot number - Ignoring");
        }
    }

    public void receivePrepareAck(PrepareAck ack) {
        if (leader) {
            return;
        }

        log("Received ACK with accept number: " + ack.getAcceptNumber());
        prepareResponses.offer(ack.getAcceptNumber());
        int size = prepareResponses.size();
        if (size > (Network.getSize()/2.0)) {
            log("Received ACK from a majority: " + size + " (Ballot: " + ack.getBallotNumber() + ")");
            Map<Integer, AcceptNumber.Acceptance> pendingRequests = new HashMap<Integer, AcceptNumber.Acceptance>();
            for (AcceptNumber acceptNumber : prepareResponses) {
                for (Integer requestNumber : acceptNumber.getRequestNumbers()) {
                    if (!log.hasValue(requestNumber)) {
                        AcceptNumber.Acceptance acceptance = pendingRequests.get(requestNumber);
                        if (acceptance != null && acceptance.getAcceptNum().compareTo(
                                acceptNumber.get(requestNumber).getAcceptNum()) > 0) {
                            pendingRequests.put(requestNumber, acceptNumber.get(requestNumber));
                        } else {
                            pendingRequests.put(requestNumber, acceptNumber.get(requestNumber));
                        }
                    }
                }
            }

            for (Integer requestNumber : pendingRequests.keySet()) {
                AcceptNumber.Acceptance acceptance = pendingRequests.get(requestNumber);
                Network.broadcast(new Accept(this, requestNumber,
                        ballotNumber, acceptance.getAcceptVal()));
            }

            int pos = 0;
            if (acceptNumber.getRequestNumbers().length > 0) {
                pos = Math.max(pos, Collections.max(Arrays.asList(acceptNumber.getRequestNumbers())));
            }
            if (pendingRequests.size() > 0) {
                pos = Math.max(pos, Collections.max(pendingRequests.keySet()));
            }

            posCounter = pos;
            log("POS Counter = " + posCounter);
            log("New leader elected");
            prepareResponses.clear();
            leader = true;
        }
    }

    public void append(Append append) {
        log("Received new tx: " + append.getTransactionId());
        int req = receiveAppend(append);
        while (!log.hasValue(req)) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {

            }
        }
        log("Transaction " + append.getTransactionId() + " committed");
    }

    public void receiveAccept(Accept accept) {
        if (accept.getBallotNumber().compareTo(ballotNumber) >= 0) {
            log("Received accept message with higher ballot number");
            acceptNumber.put(accept.getRequestNumber(), accept.getBallotNumber(), accept.getValue());
            Network.broadcast(new Accepted(this, accept.getRequestNumber(),
                    accept.getBallotNumber(), accept.getValue()));
        } else {
            log("Received accept message with a smaller ballot number - Ignoring");
        }
    }

    public void receiveAccepted(Accepted accepted) {
        if (log.hasValue(accepted.getRequestNumber())) {
            return;
        }

        LogEntry value = accepted.getValue();
        if (acceptResponses.containsKey(value)) {
            acceptResponses.put(value, acceptResponses.get(value) + 1);
        } else {
            acceptResponses.put(value, 1);
        }

        if (acceptResponses.get(value) > (Network.getSize()/2.0)) {
            log("Received a majority accepted messages");
            log("Deciding on proposal");
            log.log(accepted.getRequestNumber(), accepted.getValue());
        }
    }

    private void log(String msg) {
        System.out.println("[" + processId + "][" + System.currentTimeMillis() + "] " + msg);
        System.out.flush();
    }

    public CommitLog getLog() {
        return log;
    }
}
