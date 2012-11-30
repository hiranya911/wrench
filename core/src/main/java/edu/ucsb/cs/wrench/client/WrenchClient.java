package edu.ucsb.cs.wrench.client;

import edu.ucsb.cs.wrench.messaging.WrenchManagementService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.UUID;

public class WrenchClient {

    private static final int THREADS = 1;
    private static final int REQUESTS = 1;

    public static void main(String[] args) throws Exception {
        ClientThread[] threads = new ClientThread[THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new ClientThread();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
    }
    private static class ClientThread extends Thread {

        @Override
        public void run() {
            try {
                TTransport transport = new TSocket("localhost", 9091);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
                for (int i = 0; i < REQUESTS; i++) {
                    System.out.println(client.append(UUID.randomUUID().toString(), getName()));
                    Thread.sleep(100);
                }
                transport.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
