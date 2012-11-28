package edu.ucsb.cs.wrench.commands;

public class NullCommand extends Command {

    public static final String NULL = "NULL";

    public NullCommand() {
        super("-----");
    }

    @Override
    public String toString() {
        return NULL;
    }

    @Override
    public boolean execute() {
        return true;
    }
}
