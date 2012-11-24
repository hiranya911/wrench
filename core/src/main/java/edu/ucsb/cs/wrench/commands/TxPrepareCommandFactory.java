package edu.ucsb.cs.wrench.commands;

public class TxPrepareCommandFactory extends CommandFactory {

    public TxPrepareCommand create(String str) {
        if (str.startsWith(TxPrepareCommand.TX_PREPARE)) {
            String[] segments = str.split(" ");
            return new TxPrepareCommand(segments[1], segments[2]);
        }
        return null;
    }

}
