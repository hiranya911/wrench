package edu.ucsb.cs.wrench.config;

import edu.ucsb.cs.wrench.WrenchException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
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

    private Member[] peers = { };

    private Properties properties;

    private String wrenchHome;

    private WrenchConfiguration(Properties properties) {
        this.properties = properties;
        this.wrenchHome = System.getProperty("wrench.home", System.getProperty("user.dir"));
        Map<String,Member> membership = new HashMap<String, Member>();
        Map<String,Member> peerMembership = new HashMap<String, Member>();
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
            } else if (key.startsWith("wrench.peer.")) {
                String[] segments = key.split("\\.");
                String processId = segments[2];
                Member member;
                if (peerMembership.containsKey(processId)) {
                    member = peerMembership.get(processId);
                } else {
                    member = new Member(processId);
                    peerMembership.put(processId, member);
                }
                if ("host".equals(segments[3])) {
                    member.setHostname(properties.getProperty(key));
                } else if ("port".equals(segments[3])) {
                    member.setPort(Integer.parseInt(properties.getProperty(key)));
                }
            }
        }
        this.members = membership.values().toArray(new Member[membership.size()]);
        this.peers = peerMembership.values().toArray(new Member[peerMembership.size()]);
    }

    public String getWrenchHome() {
        return wrenchHome;
    }

    public Member[] getMembers() {
        return members;
    }

    public Member[] getPeers() {
        return peers;
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

    public String getDBDirectoryPath() {
        return properties.getProperty("wrench.db.dir", "temp");
    }

    public String getDataFileName() {
        return properties.getProperty("wrench.data.file", "DATAFILE.txt");
    }

    public void setDataFileName(String fileName) {
        properties.setProperty("wrench.data.file", fileName);
        try {
            FileUtils.touch(getDataFile());
        } catch (IOException e) {
            throw new WrenchException("Cannot initialize the data file", e);
        }
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

    public File getDataFile() {
        File dbDir = new File(getWrenchHome(), getDBDirectoryPath());
        File dataFile = new File(dbDir, getDataFileName());
        return dataFile;
    }

    public static WrenchConfiguration getConfiguration() {
        if (config == null) {
            synchronized (WrenchConfiguration.class) {
                if (config == null) {
                    String configPath = System.getProperty("wrench.config.dir", "conf");
                    Properties props = new Properties();
                    File configFile = new File(configPath, "wrench.properties");
                    try {
                        props.load(new FileInputStream(configFile));
                        config = new WrenchConfiguration(props);
                    } catch (IOException e) {
                        String msg = "Error loading Wrench configuration from: " + configFile.getPath();
                        log.error(msg, e);
                        throw new WrenchException(msg, e);
                    }
                }
            }
        }
        return config;
    }
}
