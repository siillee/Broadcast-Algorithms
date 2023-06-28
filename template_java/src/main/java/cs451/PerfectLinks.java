package cs451;

import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.HashSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 * Class implementing the Perfect Links layer of communication. This is done via UDP sockets. 
 */

public class PerfectLinks {
	
	private static final int MAX_NUMBER_OF_PROCESSES = 8;
	
	private UDP udp;
//	private UniformReliableBroadcast urb;
	private LatticeAgreement la;
//	private HashSet<Byte>[][] delivered;
	private boolean[][] delivered;
	private int numOfHosts;
	private int numOfReceivers;
	private volatile boolean stop;
	public ConcurrentSkipListSet<Message>[] sentMessages;
	private LinkedBlockingQueue<Message>[] receiveQueues;
	
	private Thread[] receivers;
	private Thread retransmitter;
	
//	private BufferedWriter bw;
	
	private int gcCount = 0;
	private long time = System.currentTimeMillis();
	
	public PerfectLinks(List<Host> hosts, int id, int numOfMessages, LatticeAgreement la) {
		
//		this.bw = bw;
//		this.urb = urb;
		this.la = la;
		
		Host host = null;
		for (Host h : hosts) {
			if (h.getId() == id) {
				host = h;
				break;
			}
		}
		
		numOfHosts = hosts.size();
		// The maximum number of threads allowed is 8, so the number of receivers needs to be calculated. 
		// The 7 subtracted represents other threads that the node will spawn (in UDP and the retransmitter and the proposer thread in LA), 
		// and the Main thread and the garbage collector. 
		numOfReceivers = MAX_NUMBER_OF_PROCESSES - 7;
		
//		delivered = new HashSet[numOfHosts][numOfMessages];
//		for (int i = 0 ; i < numOfHosts ; i++) {
//			for (int j = 0 ; j < numOfMessages ; j++) {
//				delivered[i][j] = new HashSet<Byte>();
//			}
//		}
		delivered = new boolean[numOfHosts][numOfMessages];
		
		stop = false;

		sentMessages = new ConcurrentSkipListSet[numOfHosts];
		hosts.forEach(h -> sentMessages[h.getId() - 1] = new ConcurrentSkipListSet<Message>());
		
		receiveQueues = new LinkedBlockingQueue[numOfReceivers];
		for (int i = 0 ; i < numOfReceivers ; i++) {
			receiveQueues[i] = new LinkedBlockingQueue<Message>();
		}
		
		udp = new UDP(host.getIp(), host.getPort(), hosts, this);
		
		startReceiverThreads();
		startRetransmitThread();
	}
	
	/*
	 * Send one message to a particular receiver process. 
	 */
	public void send(int msgId, byte source, byte type, byte destination, byte[] data) {
		
//		if (data == null) {
//			data = ByteBuffer.allocate(4).putInt(msgId).array();
//		}
		Message mess = new Message(msgId, source, type, destination, data, false);
		sentMessages[mess.getDestination()-1].add(mess);
		udp.send(mess);
	}
	
	/*
	 * Deliver a message by adding it to a queue. 
	 */
	public void deliver(Message mess) {
		receiveQueues[(mess.getSource()-1) % numOfReceivers].add(mess);
	}
	
	/*
	 * Starts all receiver threads. 
	 */
	private void startReceiverThreads() {
		
		receivers = new Thread[numOfReceivers];
		for (int i = 0 ; i < numOfReceivers ; i++) {
			// Need the final variable in order to use it in the thread.
			final int tmp = i;
			receivers[tmp] = new Thread(() -> {
				
				while(!stop) {
					
					try {
						Message mess = receiveQueues[tmp].take();
						
						if (stop) {
							return;
						}
						
//						byte relay = mess.isAck() ? mess.getDestination() : mess.getRelay();
						if (mess.isAck()) {
							// Removing the message because it doesn't need to be retransmitted anymore. 
							sentMessages[mess.getDestination()-1].remove(mess);
							continue;
						}else {
							// If it is being delivered now, set the ack of the message to true, and send it back, 
							// thus acknowledging to the sender that this message has been received. 
							mess.setAck(true);
							udp.send(mess);
						}
						// Check if the message has already been delivered. 
						
//						if (!delivered[mess.getSource()-1][mess.getMsgId()-1].add(relay)) {
//							continue;
//						}
						if (delivered[mess.getSource()-1][mess.getMsgId()-1]) {
							continue;
						}
						
						delivered[mess.getSource()-1][mess.getMsgId()-1] = true;
						// Deliver to lattice agreement
						la.deliver(mess.getType(), mess.getData(), mess.getSource());
						
//						urb.deliver(mess.getMsgId(), mess.getSource(), relay, mess.getData());
						
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
			receivers[i].start();
		}
	}
	
	/* 
	 * Retransmits all messages this node sent. This acts as the retransmitter in stubborn links.
	 */
	private void startRetransmitThread() {
		
		retransmitter = new Thread(() -> {
			
			while(!stop) {
				
				try {
					Thread.sleep(200);
				}catch(InterruptedException e) {
					e.printStackTrace();
				}
				
				if (stop) {
					return;
				}
				
				
												
				for(int i = 0 ; i < numOfHosts ; i++) {
					long currentTime = System.currentTimeMillis();
					if (this.time < currentTime) {
						System.gc();
						gcCount++;
						this.time = (long)(200 * Math.pow(1.5, gcCount));
					}
					for (Message m : sentMessages[i]) {
//						if (m.numOfRetransmits >= 7) {
//							sentMessages[i].remove(m);
//							continue;
//						}
						if (m.time < currentTime) {
							// Implementation of an exponential backoff mechanism. 
							m.numOfRetransmits++;
							m.time = (long) (currentTime + Math.random()*100 + 200 * Math.pow(2, m.numOfRetransmits));
							udp.send(m);
						}
					}
				}
			}
		});
		retransmitter.start();
	}
	
	public void stopLinks() {
		
		stop = true;
		udp.closeSocket();
		
		// Adding empty messages in order for the queues to not block forever.
		for (LinkedBlockingQueue<Message> tmp : receiveQueues) {
			tmp.add(new Message(0, (byte)0, (byte)0, (byte)0, null, false));
		}
		
		for (int i = 0 ; i < numOfReceivers ; i++) {
			try {
				receivers[i].join();
			}catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		try {
			retransmitter.join();
		}catch(InterruptedException e) {
			e.printStackTrace();
		}
	}
}
