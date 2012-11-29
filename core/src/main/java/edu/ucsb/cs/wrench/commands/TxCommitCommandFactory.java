package edu.ucsb.cs.wrench.commands;

public class TxCommitCommandFactory extends CommandFactory {

    @Override
    public Command create(String str) {
        if (str.startsWith(TxCommitCommand.TX_COMMIT)) {
            String segment = str.substring(TxCommitCommand.TX_COMMIT.length());
            int delimiter = segment.indexOf(' ');
            return new TxCommitCommand(segment.substring(0, delimiter),
                    Long.parseLong(segment.substring(delimiter + 1)));
        }
        return null;
    }
}
