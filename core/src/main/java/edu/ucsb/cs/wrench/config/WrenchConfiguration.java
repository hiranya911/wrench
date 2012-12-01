package edu.ucsb.cs.wrench.config;

import edu.ucsb.cs.wrench.WrenchException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class WrenchConfiguration {

    private static final Log log = LogFactory.getLog(WrenchConfiguration.class);

    private static volatile WrenchConfiguration config = null;

    private Member[] members = {
        new Member("DefaultNode", "localhost", 9090, true)
    };

    private Member[] peers = { };

    private Properties properties;

    private String wrenchHome;

    private String clusterName;

    private WrenchConfiguration(Properties properties) {
        this.properties = properties;
        this.wrenchHome = System.getProperty("wrench.home", System.getProperty("user.dir"));
        this.clusterName = System.getProperty("wrench.cluster.name");
        if (this.clusterName == null) {
            throw new WrenchException("wrench.cluster.name not specified");
        }

        File zkDir = new File(System.getProperty("wrench.zk.dir"));
        File myIdFile = new File(zkDir, "myid");
        String myId = null;
        try {
            myId = FileUtils.readFileToString(myIdFile).trim();
        } catch (IOException e) {
            throw new WrenchException("Unable to read the ZK myid file", e);
        }

        List<Member> allMembers = new ArrayList<Member>();
        List<Member> allPeers = new ArrayList<Member>();
        for (String property : properties.stringPropertyNames()) {
            if (property.startsWith("wrench.server.")) {
                String prefix = property.substring("wrench.server.".length(), property.lastIndexOf('.'));
                if (prefix.equals(this.clusterName)) {
                    String value = properties.getProperty(property);
                    String processId = property.substring(property.lastIndexOf('.') + 1);
                    String[] connection = value.split(":");
                    boolean local = processId.equals(myId);
                    Member member = new Member(prefix + processId, connection[0],
                            Integer.parseInt(connection[1]), local);
                    allMembers.add(member);
                } else {
                    String value = properties.getProperty(property);
                    String processId = property.substring(property.lastIndexOf('.') + 1);
                    String[] connection = value.split(":");
                    Member member = new Member(prefix + processId, connection[0],
                            Integer.parseInt(connection[1]), false);
                    allPeers.add(member);
                }
            }
        }

        this.members = allMembers.toArray(new Member[allMembers.size()]);
        this.peers = allPeers.toArray(new Member[allPeers.size()]);
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
