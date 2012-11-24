package edu.ucsb.cs.wrench.config;

public class Member implements Comparable<Member> {

    private String processId;
    private String hostname;
    private int port;
    private boolean local;

    public Member(String processId) {
        this.processId = processId;
    }

    public Member(String processId, String hostname, int port, boolean local) {
        this(processId);
        this.hostname = hostname;
        this.port = port;
        this.local = local;
    }

    void setHostname(String hostname) {
        this.hostname = hostname;
    }

    void setPort(int port) {
        this.port = port;
    }

    void setLocal(boolean local) {
        this.local = local;
    }

    public String getProcessId() {
        return processId;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public boolean isLocal() {
        return local;
    }

    @Override
    public int compareTo(Member o) {
        return processId.compareTo(o.processId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Member) {
            return ((Member) obj).compareTo(this) == 0;
        }
        return false;
    }
}
