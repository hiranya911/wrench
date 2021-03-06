package edu.ucsb.cs.wrench.commands;

import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TxCommitCommand extends Command {

    public static final String TX_COMMIT = "TX_COMMIT ";

    private long lineNumber = -1L;

    public TxCommitCommand(String transactionId) {
        super(transactionId);
    }

    @Override
    public boolean execute() {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        File tempDir = new File(config.getWrenchHome(), config.getTempDirectoryPath());
        File tempFile = new File(tempDir, transactionId + ".dat");

        File databaseDir = new File(config.getWrenchHome(), config.getDBDirectoryPath());
        File dataFile = new File(databaseDir, config.getDataFileName());
        try {
            String data = FileUtils.readFileToString(tempFile).trim();
            List<String> lines = FileUtils.readLines(dataFile);
            if (lineNumber == lines.size() + 1) {
                FileUtils.writeStringToFile(dataFile, data + "\n", true);
                if (log.isDebugEnabled()) {
                    log.debug("Transaction " + transactionId + " COMMITTED");
                }
                synchronized (this) {
                    this.notifyAll();
                }
                FileUtils.deleteQuietly(tempFile);
                return true;
            }
        } catch (IOException e) {
            log.fatal("Error while performing the file system operations", e);
        }
        return false;
    }

    public void setLineNumber(long lineNumber) {
        this.lineNumber = lineNumber;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TxCommitCommand) {
            TxCommitCommand commit = (TxCommitCommand) o;
            return commit.transactionId.equals(transactionId) &&
                    commit.lineNumber == lineNumber;
        }
        return false;
    }

    @Override
    public String toString() {
        return TX_COMMIT + transactionId + " " + lineNumber;
    }
}
