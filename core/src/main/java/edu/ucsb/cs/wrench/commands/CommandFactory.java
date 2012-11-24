package edu.ucsb.cs.wrench.commands;

import edu.ucsb.cs.wrench.WrenchException;

public abstract class CommandFactory {

    private static CommandFactory[] factories = {
        new TxPrepareCommandFactory()
    };

    public abstract Command create(String str);

    public static Command createCommand(String str) {
        for (CommandFactory factory : factories) {
            Command command = factory.create(str);
            if (command != null) {
                return command;
            }
        }
        throw new WrenchException("Unknown command string: " + str);
    }

}
