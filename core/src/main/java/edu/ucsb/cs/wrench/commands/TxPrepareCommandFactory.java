package edu.ucsb.cs.wrench.commands;

public class TxPrepareCommandFactory extends CommandFactory {

    public TxPrepareCommand create(String str) {
        if (str.startsWith(TxPrepareCommand.TX_PREPARE)) {
            String segment = str.substring(TxPrepareCommand.TX_PREPARE.length());
            int delimiter = segment.indexOf(' ');
            return new TxPrepareCommand(segment.substring(0, delimiter),
                    segment.substring(delimiter + 1));
        }
        return null;
    }

}
