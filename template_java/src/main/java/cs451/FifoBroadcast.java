package cs451;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

/*
 * Class implementing the FIFO broadcast. 
 */

public class FifoBroadcast {

	public UniformReliableBroadcast urb;
	private int[] next;
	// Mapping of source id to message id
	private HashMap<Byte, ConcurrentSkipListSet<Integer>> pending;
	
	private BufferedWriter bw;
	
	public FifoBroadcast(List<Host> hosts, int id, int m, BufferedWriter bw) {
		
		this.bw = bw;
		
		next = new int[hosts.size()];
		for (int i = 0 ; i < next.length ; i++) {
			next[i] = 1;
		}
		
		pending = new HashMap<Byte, ConcurrentSkipListSet<Integer>>();
		for (byte i = 1 ; i <= hosts.size() ; i++) {
			pending.put(i, new ConcurrentSkipListSet<Integer>());
		}
	
		
		urb = new UniformReliableBroadcast(hosts, id, m, bw, this);
	}
	
	/*
	 * Broadcasts the message using the underlying URB layer. 
	 */
	public void broadcast() {
		urb.broadcast();
	}
	
	/*
	 * Delivery of a message at this level of communication. 
	 */
	public void deliver(int msgId, byte source, byte relay, byte[] data) {
		
		ConcurrentSkipListSet<Integer> pendingFromSource = pending.get(source);
		pendingFromSource.add(msgId);
		
		if (next[source-1] == msgId) {
			for(Integer nextMsgId : pendingFromSource) {
				if (nextMsgId == next[source-1]) {
					next[source-1]++;
					try {
						bw.write("d " + source + " " + nextMsgId + "\n");
					}catch(IOException e) {
						e.printStackTrace();
					}	
					pendingFromSource.remove(nextMsgId);
				}
			}
		}
	}
	
	public void stopBroadcast() {
		urb.stopBroadcast();
	}
}
