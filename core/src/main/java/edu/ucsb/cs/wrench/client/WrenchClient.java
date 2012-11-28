package edu.ucsb.cs.wrench.client;

import edu.ucsb.cs.wrench.messaging.WrenchManagementService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.UUID;

public class WrenchClient {

    public static void main(String[] args) throws Exception {
        //TTransport transport = new TSocket("localhost", 9091);
        //transport.open();
        //TProtocol protocol = new TBinaryProtocol(transport);
        //WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
        //System.out.println(client.append(UUID.randomUUID().toString(), "hello world 3"));
        ClientThread[] threads = new ClientThread[5];
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
                System.out.println(client.append(UUID.randomUUID().toString(), getName()));
                transport.close();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

}
