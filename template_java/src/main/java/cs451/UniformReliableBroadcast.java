package cs451;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

/*
 * Class implementing the Uniform Reliable broadcast. 
 */

public class UniformReliableBroadcast {
	
	private List<Host> hosts;
	public PerfectLinks pl;
	private FifoBroadcast fb;
	private byte[][] ack;
	private boolean[][] delivered;
	
	private int myId;
	private int m;
	
	private BufferedWriter bw;
	
	public UniformReliableBroadcast(List<Host> hosts, int id, int m, BufferedWriter bw, FifoBroadcast fb) {
		
		this.myId = id;
		this.hosts = hosts;
		this.bw = bw;
		this.m = m;
		this.fb = fb;
		
		ack = new byte[hosts.size()][m];
		
		delivered = new boolean[hosts.size()][m];
		
//		pl = new PerfectLinks(hosts, id, m, this);
	}
	
	/*
	 * Broadcasts all messages that this process has to send. This is done by sending each message to each
	 * other process in the system, via the underlying Perfect Links layer. 
	 */
	public void broadcast() {
		

		for (int i = 1 ; i <= m ; i++) {
			try {
				Thread.sleep(hosts.size()*15);
			}catch(InterruptedException e) {
				e.printStackTrace();
			}
			try {
       			bw.write("b " + i + "\n");
       		}catch(IOException e) {
       			e.printStackTrace();
       		}
			for (Host h : hosts) {
				// The payload is just an integer, however this can be changed for whatever payload is needed.
				pl.send(i, (byte)myId, (byte)myId, (byte)h.getId(), null);
			}
        }
	}
	
	/*
	 * Delivery of a message at this level of communication. 
	 */
	public void deliver(int msgId, byte source, byte relay, byte[] data) {
		
		ack[source-1][msgId-1]++;
		
		if (ack[source-1][msgId-1] == 1 && source != myId) {
			for (Host h : hosts) {
				pl.send(msgId, source, (byte)myId, (byte)h.getId(), data);
			}
		}
		
		if (ack[source-1][msgId-1] > hosts.size()/2 && !delivered[source-1][msgId-1]) {
			fb.deliver(msgId, source, relay, data);
			delivered[source-1][msgId-1] = true;
		}
	}
	
	public void stopBroadcast() {
		pl.stopLinks();
	}
	
}
