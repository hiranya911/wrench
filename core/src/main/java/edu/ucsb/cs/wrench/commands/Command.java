package edu.ucsb.cs.wrench.commands;

import edu.ucsb.cs.wrench.WrenchException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Command {

    protected final Log log = LogFactory.getLog(this.getClass());

    protected String transactionId;

    protected Command(String transactionId) {
        if (transactionId.contains(" ")) {
            throw new WrenchException("Invalid transaction ID: " + transactionId);
        }
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public abstract boolean execute();

    public abstract String toString();
}
