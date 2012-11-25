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
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class PaxosLedger {

    private static final Log log = LogFactory.getLog(PaxosLedger.class);

    private static final String LAST_TRIED = "LAST_TRIED ";
    private static final String NEXT_BALLOT = "NEXT_BALLOT ";
    private static final String PREV_BALLOT = "PREV_BALLOT ";
    private static final String OUTCOME = "OUTCOME ";
    private static final String EXECUTION = "EXECUTION ";

    private File ledgerFile;

    private BallotNumber lastTried;
    private LRUCache<Long,BallotNumber> prevBal = new LRUCache<Long, BallotNumber>(10);
    private LRUCache<Long,Command> prevCommand = new LRUCache<Long, Command>(10);
    private LRUCache<Long,BallotNumber> nextBal = new LRUCache<Long, BallotNumber>(10);
    private LRUCache<Long,Command> outcome = new LRUCache<Long, Command>(10);
    private long lastExecuted = -1;

    public PaxosLedger() throws IOException {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        ledgerFile = new File(config.getWrenchHome(), config.getLedgerPath());
        FileUtils.touch(ledgerFile);
    }

    public void logLastTriedBallotNumber(BallotNumber bal) {
        append(LAST_TRIED + bal);
        lastTried = bal;
    }

    public void logNextBallotNumber(long requestNumber, BallotNumber bal) {
        append(NEXT_BALLOT + requestNumber + " " + bal);
        nextBal.put(requestNumber, bal);
    }

    public void logAcceptance(long requestNumber, BallotNumber bal, Command command) {
        append(PREV_BALLOT + requestNumber + " " + bal + " [" + command + "]");
        prevBal.put(requestNumber, bal);
        prevCommand.put(requestNumber, command);
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
        if (lastTried != null) {
            return lastTried;
        } else {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(LAST_TRIED)) {
                    lastTried = new BallotNumber(line.split(" ")[1]);
                    return lastTried;
                }
            }
        }
        return null;
    }

    public BallotNumber getNextBallotNumber(long requestNumber) {
        BallotNumber next = nextBal.get(requestNumber);
        if (next != null) {
            return next;
        } else {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(NEXT_BALLOT + requestNumber + " ")) {
                    next = new BallotNumber(line.split(" ")[2]);
                    nextBal.put(requestNumber, next);
                }
            }
        }
        return next;
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
                    String command = line.substring(line.indexOf('[') + 1,
                            line.lastIndexOf(']'));
                    prevBal.put(requestNumber, prev);
                    prevCommand.put(requestNumber, CommandFactory.createCommand(command));
                }
            }
        }
        return prev;
    }

    public Command getPreviousCommand(long requestNumber) {
        Command prev = prevCommand.get(requestNumber);
        if (prev != null) {
            return prev;
        } else {
            ListIterator<String> iterator = listIterator();
            while (iterator.hasPrevious()) {
                String line = iterator.previous();
                if (line.startsWith(PREV_BALLOT + requestNumber + " ")) {
                    String command = line.substring(line.indexOf('[') + 1,
                            line.lastIndexOf(']'));
                    prev = CommandFactory.createCommand(command);
                    prevCommand.put(requestNumber, prev);
                    prevBal.put(requestNumber, new BallotNumber(line.split(" ")[2]));
                }
            }
        }
        return prev;
    }

    public Map<Long,BallotNumber> getPreviousBallotNumbers(long requestNumber) {
        Map<Long,BallotNumber> prevBallots = new HashMap<Long, BallotNumber>();
        for (long i = requestNumber; i <= getLastExecutedRequest(); i++) {
            BallotNumber prevBal = getPreviousBallotNumber(i);
            if (prevBal != null) {
                prevBallots.put(i, prevBal);
            }
        }
        return prevBallots;
    }

    public Map<Long,Command> getPreviousCommands(long requestNumber) {
        Map<Long,Command> prevCommands = new HashMap<Long,Command>();
        for (long i = requestNumber; i <= getLastExecutedRequest(); i++) {
            Command prevCmd = getPreviousCommand(i);
            if (prevCmd != null) {
                prevCommands.put(i, prevCmd);
            }
        }
        return prevCommands;
    }

    public Map<Long,Command> getPreviousOutcomes(long requestNumber) {
        Map<Long,Command> prevCommands = new HashMap<Long,Command>();
        for (long i = requestNumber; i <= getLastExecutedRequest(); i++) {
            Command prevCmd = getOutcome(i);
            if (prevCmd != null) {
                prevCommands.put(i, prevCmd);
            }
        }
        return prevCommands;
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
        if (lastExecuted == -1) {
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
