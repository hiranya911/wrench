package edu.ucsb.cs.wrench.paxos;

import java.util.ArrayList;
import java.util.List;

public class DatabaseSnapshot {

    private List<String> grades = new ArrayList<String>();
    private List<String> stats = new ArrayList<String>();

    public void addData(String gradesLine, String statsLine) {
        grades.add(gradesLine);
        stats.add(statsLine);
    }

    public List<String> getGrades() {
        return grades;
    }

    public List<String> getStats() {
        return stats;
    }
}
