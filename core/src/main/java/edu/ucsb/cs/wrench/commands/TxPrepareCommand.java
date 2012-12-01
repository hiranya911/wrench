package edu.ucsb.cs.wrench.commands;

import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class TxPrepareCommand extends Command {

    public static final String TX_PREPARE = "TX_PREPARE ";

    private String data;

    public TxPrepareCommand(String transactionId, String data) {
        super(transactionId);
        this.data = data;
    }

    @Override
    public boolean execute() {
        WrenchConfiguration config = WrenchConfiguration.getConfiguration();
        File tempDir = new File(config.getWrenchHome(), config.getTempDirectoryPath());
        File file = new File(tempDir, transactionId + ".dat");
        try {
            FileUtils.writeStringToFile(file, data);
            if (log.isDebugEnabled()) {
                log.debug("Transaction " + transactionId + " PREPARED");
            }
            return true;
        } catch (IOException e) {
            log.error("Error writing to the file system", e);
            return false;
        }
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return TX_PREPARE + transactionId + " " + data;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TxPrepareCommand) {
            TxPrepareCommand prep = (TxPrepareCommand) obj;
            return prep.transactionId.equals(transactionId) &&
                    prep.data.equals(data);
        }
        return false;
    }
}
