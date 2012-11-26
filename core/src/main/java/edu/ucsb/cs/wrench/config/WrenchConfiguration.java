package edu.ucsb.cs.wrench.config;

import edu.ucsb.cs.wrench.WrenchException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WrenchConfiguration {

    private static final Log log = LogFactory.getLog(WrenchConfiguration.class);

    private static volatile WrenchConfiguration config = null;

    private Member[] members = {
        new Member("DefaultNode", "localhost", 9090, true)
    };

    private Properties properties;

    private String wrenchHome;

    private WrenchConfiguration(Properties properties) {
        this.properties = properties;
        this.wrenchHome = System.getProperty("wrench.home", System.getProperty("user.dir"));
        Map<String,Member> membership = new HashMap<String, Member>();
        boolean localSeen = false;
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("wrench.member.")) {
                String[] segments = key.split("\\.");
                String processId = segments[2];
                Member member;
                if (membership.containsKey(processId)) {
                    member = membership.get(processId);
                } else {
                    member = new Member(processId);
                    membership.put(processId, member);
                }
                if ("host".equals(segments[3])) {
                    member.setHostname(properties.getProperty(key));
                } else if ("port".equals(segments[3])) {
                    member.setPort(Integer.parseInt(properties.getProperty(key)));
                } else if ("local".equals(segments[3])) {
                    boolean local = Boolean.parseBoolean(properties.getProperty(key));
                    if (local && localSeen) {
                        throw new WrenchException("Only one member be designated local");
                    }
                    member.setLocal(local);
                    if (local) {
                        localSeen = true;
                    }
                }
            }
        }
        this.members = membership.values().toArray(new Member[membership.size()]);
    }

    public String getWrenchHome() {
        return wrenchHome;
    }

    public Member[] getMembers() {
        return members;
    }

    public Member getLocalMember() {
        for (Member member : members) {
            if (member.isLocal()) {
                return member;
            }
        }
        throw new WrenchException("No local member");
    }

    public String getLedgerPath() {
        return properties.getProperty("wrench.ledger.path", "ledger.dat");
    }

    public String getTempDirectoryPath() {
        return properties.getProperty("wrench.temp.dir", "temp");
    }

    public int getLeaderElectionTimeout() {
        return Integer.parseInt(properties.getProperty("wrench.election.timeout", "30000"));
    }

    public int getServerPort() {
        for (Member member : members) {
            if (member.isLocal()) {
                return member.getPort();
            }
        }
        return -1;
    }

    public Member getMember(String processId) {
        for (Member member : members) {
            if (member.getProcessId().equals(processId)) {
                return member;
            }
        }
        throw new WrenchException("Unknown member: " + processId);
    }

    public boolean isMajority(int count) {
        return count > members.length/2.0;
    }

    public static WrenchConfiguration getConfiguration() {
        if (config == null) {
            synchronized (WrenchConfiguration.class) {
                if (config == null) {
                    String configPath = System.getProperty("wrench.config.file", "wrench.properties");
                    Properties props = new Properties();
                    try {
                        props.load(new FileInputStream(configPath));
                        config = new WrenchConfiguration(props);
                    } catch (IOException e) {
                        String msg = "Error loading Wrench configuration from: " + configPath;
                        log.error(msg, e);
                        throw new WrenchException(msg, e);
                    }
                }
            }
        }
        return config;
    }
}
