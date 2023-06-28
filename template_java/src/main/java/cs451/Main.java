package cs451;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class Main {

	private static BufferedWriter buffer;
	private static FileWriter f;
//	private static PerfectLinks pl;
//	private static UniformReliableBroadcast urb;
//	private static FifoBroadcast fb;	
	private static LatticeAgreement la;
	
    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
//        pl.stopLinks();
//        urb.stopBroadcast();
//        fb.stopBroadcast();
        la.stopProposing();

        //write/flush output file if necessary
        System.out.println("Writing output.");
        try {
        	buffer.close();
        	f.close();
        }catch(IOException e) {
        	e.printStackTrace();
        }
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");
        
        // Number of messages to send.
//        int m = 0;
        // Information about the proposals (number of proposals, max number of elements per proposal and
        // max number of distinct elements in the entire file ; respectively p, vs, ds). 
        int p = 0;
        int vs = 0;
        int ds = 0;
        BufferedReader br;
        
        // Initialization of the output buffer. 
        try {
        	File outputFile = new File(parser.output());
        	f = new FileWriter(outputFile);
        	buffer = new BufferedWriter(f);
        }catch(IOException e) {
        	System.out.println("Error while initializing the output buffer");
        	e.printStackTrace();
        	System.exit(1);
        }
        
        HashSet<Integer>[] proposals = new HashSet[1];
        // Reading the config file. 
        try {
//        	m = Integer.parseInt(br.readLine());      // this was for perfect links and fifo
        	br = new BufferedReader(new FileReader(parser.config()));
        	String[] tokens = br.readLine().split(" ");
        	p = Integer.parseInt(tokens[0].trim());
        	vs = Integer.parseInt(tokens[1].trim());
        	ds = Integer.parseInt(tokens[2].trim());
        	proposals = new HashSet[p];
        	for (int i = 0 ; i < p ; i++) {
        		proposals[i] = new HashSet<Integer>();
        		tokens = br.readLine().split(" ");
        		for (String x : tokens) {
        			proposals[i].add(Integer.parseInt(x.trim()));
        		}
        	}
        	
        	br.close();
        }catch(IOException e) {
        	e.printStackTrace();
        }
        
//        pl = new PerfectLinks(parser.hosts(), parser.myId(), m, buffer);
//        urb = new UniformReliableBroadcast(parser.hosts(), parser.myId(), m, buffer);
//        fb = new FifoBroadcast(parser.hosts(), parser.myId(), m, buffer);
        la = new LatticeAgreement(proposals, parser.hosts(), parser.myId(), buffer);
        
        System.out.println("Broadcasting and delivering messages...\n");
        
        try {
    		Thread.sleep(3000);
    	}catch(InterruptedException e) {
    		e.printStackTrace();
    	}
        
//      pl.send(mess);
//        urb.broadcast();
//        fb.broadcast();
        la.propose(0);

        
        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
