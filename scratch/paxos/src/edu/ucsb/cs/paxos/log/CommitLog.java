package edu.ucsb.cs.paxos.log;

import java.util.Map;
import java.util.TreeMap;

public class CommitLog {

    private Map<Integer,LogEntry> log = new TreeMap<Integer, LogEntry>();

    public synchronized void log(int index, LogEntry entry) {
        log.put(index, entry);
    }

    public synchronized LogEntry[] getEntries() {
        return log.values().toArray(new LogEntry[log.size()]);
    }

    public synchronized int getNextPosition() {
        return log.size();
    }

    public synchronized boolean hasValue(int position) {
        return log.containsKey(position);
    }

    public synchronized String toString() {
        StringBuilder b = new StringBuilder();
        for (Integer index : log.keySet()) {
            b.append(index).append(" => ").append(log.get(index)).append("\n");
        }
        return b.toString();
    }
}
