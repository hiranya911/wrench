package edu.ucsb.cs.wrench.paxos;

import edu.ucsb.cs.wrench.WrenchException;
import edu.ucsb.cs.wrench.commands.Command;
import edu.ucsb.cs.wrench.commands.CommandFactory;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.utils.LRUCache;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

public class PaxosLedger {

    private static final Log log = LogFactory.getLog(PaxosLedger.class);

    private static final String LAST_TRIED = "LAST_TRIED ";
    private static final String NEXT_BALLOT = "NEXT_BALLOT ";
    private static final String PREV_BALLOT = "PREV_BALLOT ";
    private static final String OUTCOME = "OUTCOME ";
    private static final String EXECUTION = "EXECUTION ";

    private File ledgerFile;

    private BallotNumber lastTried;
    private BallotNumber nextBal;
    private LRUCache<Long,BallotNumber> prevBal = new LRUCache<Long, BallotNumber>(10);
    private LRUCache<Long,Command> outcome = new LRUCache<Long, Command>(10);
    private long lastExecuted = 0;

    public PaxosLedger() throws IOException {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        ledgerFile = new File(config.getWrenchHome(), config.getLedgerPath());
        FileUtils.touch(ledgerFile);
    }

    public void logLastTriedBallotNumber(BallotNumber bal) {
        append(LAST_TRIED + bal);
        lastTried = bal;
    }

    public void logNextBallotNumber(BallotNumber bal) {
        append(NEXT_BALLOT + bal);
        nextBal = bal;
    }

    public void logAcceptance(long requestNumber, BallotNumber bal, Command command) {
        append(PREV_BALLOT + requestNumber + " " + bal + " [" + command + "]");
        prevBal.put(requestNumber, bal);
    }

    public void logOutcome(long requestNumber, Command command) {
        append(OUTCOME + requestNumber + " [" + command + "]");
        outcome.put(requestNumber, command);
    }

    public void logExecution(long requestNumber) {
        append(EXECUTION + requestNumber);
        lastExecuted = requestNumber;
    }

    public BallotNumber getLastTriedBallotNumber() {
        if (lastTried == null) {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(LAST_TRIED)) {
                    lastTried = new BallotNumber(line.split(" ")[1]);
                    break;
                }
            }
        }
        return lastTried;
    }

    public BallotNumber getNextBallotNumber() {
        if (nextBal == null) {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(NEXT_BALLOT)) {
                    nextBal = new BallotNumber(line.split(" ")[1]);
                    break;
                }
            }
        }
        return nextBal;
    }

    public BallotNumber getPreviousBallotNumber(long requestNumber) {
        BallotNumber prev = prevBal.get(requestNumber);
        if (prev != null) {
            return prev;
        } else {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(PREV_BALLOT + requestNumber + " ")) {
                    prev = new BallotNumber(line.split(" ")[2]);
                    prevBal.put(requestNumber, prev);
                }
            }
        }
        return prev;
    }

    public RequestHistory getRequestHistory(long start) {
        RequestHistory history = new RequestHistory();
        ListIterator<String> iterator = listIterator();
        while (iterator.hasPrevious()) {
            String line = iterator.previous();
            if (line.startsWith(PREV_BALLOT)) {
                String[] segments = line.split(" ");
                long requestNumber = Long.parseLong(segments[1]);
                if (requestNumber > start) {
                    BallotNumber prev = new BallotNumber(line.split(" ")[2]);
                    Command command = CommandFactory.createCommand(line.substring(
                            line.lastIndexOf('[') + 1, line.lastIndexOf(']')));
                    history.addPreviousBallotNumber(requestNumber, prev);
                    history.addPreviousCommand(requestNumber, command);
                }
            } else if (line.startsWith(OUTCOME)) {
                String[] segments = line.split(" ");
                long requestNumber = Long.parseLong(segments[1]);
                if (requestNumber > start) {
                    Command command = CommandFactory.createCommand(line.substring(
                            line.indexOf('[') + 1, line.lastIndexOf(']')));
                    history.addPreviousOutcome(requestNumber, command);
                }
            }
        }

        return history;
    }

    public Command getOutcome(long requestNumber) {
        Command o = outcome.get(requestNumber);
        if (o != null) {
            return o;
        } else {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(OUTCOME + requestNumber + " [")) {
                    String command = line.substring(line.indexOf('[') + 1,
                            line.lastIndexOf(']'));
                    o = CommandFactory.createCommand(command);
                    outcome.put(requestNumber, o);
                }
            }
        }
        return o;
    }

    public long getLastExecutedRequest() {
        if (lastExecuted == 0) {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(EXECUTION)) {
                    lastExecuted = Long.parseLong(line.split(" ")[1]);
                    return lastExecuted;
                }
            }
        }
        return lastExecuted;
    }

    public long getLargestDecidedRequest() {
        long lastDecided = -1;
        ListIterator<String> iterator = listIterator();
        while (iterator.hasPrevious()) {
            String line = iterator.previous();
            if (line.startsWith(OUTCOME)) {
                long requestNumber = Long.parseLong(line.split(" ")[1]);
                if (requestNumber > lastDecided) {
                    lastDecided = requestNumber;
                }
            }
        }
        return lastDecided;
    }


    private ListIterator<String> listIterator() {
        try {
            List<String> lines = FileUtils.readLines(ledgerFile);
            return lines.listIterator(lines.size());
        } catch (IOException e) {
            handleException("Error reading from the file system", e);
            return null;
        }
    }

    private void append(String msg) {
        try {
            FileUtils.writeStringToFile(ledgerFile, msg + "\n", true);
        } catch (IOException e) {
            handleException("Error writing to the file system", e);
        }
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new WrenchException(msg, e);
    }
}
