package edu.ucsb.cs.wrench;

public class WrenchException extends RuntimeException {

    public WrenchException(String message) {
        super(message);
    }

    public WrenchException(String message, Throwable cause) {
        super(message, cause);
    }
}
