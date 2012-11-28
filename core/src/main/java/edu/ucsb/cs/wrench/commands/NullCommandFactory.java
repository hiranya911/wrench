package edu.ucsb.cs.wrench.commands;

import edu.ucsb.cs.wrench.WrenchException;

public class NullCommandFactory extends CommandFactory {

    @Override
    public Command create(String str) {
        if (str.equals(NullCommand.NULL)) {
            return new NullCommand();
        }
        return null;
    }
}
