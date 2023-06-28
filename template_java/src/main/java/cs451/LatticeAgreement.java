package cs451;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * Class implementing the Lattice Agreement. The implementation is done using Threshold Logical Clocks (TLC).
 * TLC messages are used to make sure each iteration of the algorithm is executed correctly, 
 * more precisely that no values from execution i-1 overflow into execution i.  
 */
public class LatticeAgreement {
	
	private volatile boolean active;
	private volatile int ackCount;
	private volatile int nackCount;
	private volatile int activeProposalNum;
	private volatile HashSet<Integer> proposedValue;
	private volatile HashSet<Integer> acceptedValue;
	
	private int myId;
	private List<Host> hosts;
	private HashSet<Integer>[] proposals;
	private PerfectLinks pl;
	private BufferedWriter bw;
	
	private volatile int msgId;
	private volatile int currentProposal;
	
	private volatile int currentStep;
	private ConcurrentHashMap<Integer, LinkedList<TLC_Message>> storedTLCs;
	
	private Thread proposer;
	private boolean stop = false;
	private LinkedBlockingQueue<Integer> proposalQueue;

	private boolean[] decided;
	
	public LatticeAgreement(HashSet<Integer>[] proposals, List<Host> hosts, int myId, BufferedWriter bw) {
		
		storedTLCs = new ConcurrentHashMap<Integer, LinkedList<TLC_Message>>();
		proposalQueue = new LinkedBlockingQueue<Integer>();
		
		pl = new PerfectLinks(hosts, myId, proposals.length*hosts.size()*10, this);
		
		this.proposals = proposals;
		this.bw = bw;
		this.myId = myId;
		this.hosts = hosts;
		
		active = false;
		ackCount = 0;
		nackCount = 0;
		activeProposalNum = 0;
		proposedValue = new HashSet<Integer>();
		acceptedValue = new HashSet<Integer>();
		
		msgId = 0;
		currentProposal = 0;
		
		currentStep = 0;
		
		decided = new boolean[proposals.length];
		
		startProposeThread();
	}

	/*
	 * Propose step in the algorithm. 
	 */
	public void propose(int index) {
		
		resetState();
		System.gc();	
		if (index >= proposals.length) {
			return;
		}
		proposalQueue.add(index);
	}
	
	public void startProposeThread() {
		
		proposer = new Thread(() -> {
			
			while(!stop) {
				
				try {
					int index = proposalQueue.take();
					
					if (stop) {
						return;
					}
					
//					proposedValue = proposals[index];
					synchronized(proposedValue) {
						proposedValue.addAll(proposals[index]);
					}
					active = true;
					activeProposalNum++;
					ackCount = 0;
					nackCount = 0;
					
					msgId++;
					
					broadcast(proposedValue, activeProposalNum, msgId);
					
				}catch(InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		proposer.start();
	}
	
	/*
	 * Broadcasts a message to all other processes by sending it to each other process, 
	 * via the underlying Perfect Links layer. 
	 */
	private void broadcast(HashSet<Integer> value, int activeProposalNum, int msgId) {
		
		synchronized(proposedValue) {
			ByteBuffer buffer = ByteBuffer.allocate((value.size() + 2) * 4);
			buffer.putInt(currentStep);
			buffer.putInt(activeProposalNum);
			value.forEach(x -> buffer.putInt(x.intValue()));
			byte[] data = buffer.array();
			
			for(Host h : hosts) {
				pl.send(msgId, (byte)myId, (byte)0, (byte)h.getId(), data);
			}
		}
	}
	
	/*
	 * Delivery of message at this (highest) level. 
	 */
	public void deliver(byte type, byte[] data, byte source) {
		
		ByteBuffer buffer = ByteBuffer.wrap(data);
		int step = buffer.getInt();
		
		// Check if this process is some step(s) behind the overall execution. 
		if (step < currentStep) {
			if (type == 0) {
				sendTLC(source, step);
			}
			return;
		}
		
		int proposalNum = 0;
		if (type != 3) {
			proposalNum = buffer.getInt();
		}
		
		HashSet<Integer> value = new HashSet<Integer>();
		while(buffer.hasRemaining()) {
			value.add(buffer.getInt());
		}
		
		// We have a different behaviour based on which message has been delivered. 
		switch(type) {
		case 1:
			deliverAck(proposalNum);
			break;
		case 2:
			deliverNack(value, proposalNum);
			break;
		case 3:
			deliverTLC(data, source);
			break;
		default:
			deliverProposal(value, proposalNum, source);
		}
	}
	
	/*
	 * Send a TLC message to a specific process. This is used when that process is behind and needs to catch up to 
	 * other processes, which are steps in front of it. 
	 */
	private void sendTLC(byte destination, int step) {

		Optional<TLC_Message> tlcMsg = storedTLCs.get(step).stream().filter(x -> x.getSource() == (byte)myId).findFirst();
		if (tlcMsg.isEmpty()) {
			return;
		}
		
		HashSet<Integer> value = tlcMsg.get().getValue();
		ByteBuffer buffer = ByteBuffer.allocate((value.size() + 1)*4); 
		buffer.putInt(step);
		value.forEach(x -> buffer.putInt(x.intValue()));
		byte[] data = buffer.array();
		msgId++;
		pl.send(msgId, (byte)myId, (byte)3, destination, data);
	}
	
	/*
	 * Delivery of a proposal from another process. 
	 */
	private void deliverProposal(HashSet<Integer> value, int proposalNum, byte source) {
		
		synchronized(acceptedValue) {
			if (value.containsAll(acceptedValue)) {
					acceptedValue = value;
					msgId++;
					byte[] data = ByteBuffer.allocate(8).putInt(currentStep).putInt(proposalNum).array();
					// Send ACK back to source.
					pl.send(msgId, (byte)myId, (byte)1, source, data);
			}else {
					acceptedValue.addAll(value);
					ByteBuffer buffer = ByteBuffer.allocate((acceptedValue.size() + 2)*4);
					buffer.putInt(currentStep);
					buffer.putInt(proposalNum);
					acceptedValue.forEach(x -> buffer.putInt(x.intValue()));
					byte[] data = buffer.array();
					msgId++;
					// Send NACK back to source.
					pl.send(msgId, (byte)myId, (byte)2, source, data);
			}
		}
	}
	
	/*
	 * Delivery of an ack from another process. 
	 */
	private void deliverAck(int proposalNum) {
		
		if (proposalNum == activeProposalNum) {
			ackCount++;
		}
		
		if (ackCount > hosts.size()/2 && active) {
			synchronized(proposedValue) {
				decide(proposedValue);
			}
			active = false;
			}
	}
	
	/*
	 * Delivery of a nack from another process. 
	 */
	private void deliverNack(HashSet<Integer> value, int proposalNum) {
		
		if (proposalNum == activeProposalNum) {
			synchronized(proposedValue) {
				proposedValue.addAll(value);
				nackCount++;
			}
		}
		
		if (nackCount > 0 && (ackCount+nackCount > hosts.size()/2) && active) {
			activeProposalNum++;
			ackCount = 0;
			nackCount = 0;
			
			msgId++;
			broadcast(proposedValue, activeProposalNum, msgId);
		}
	}
	
	/*
	 * Delivery of a TLC message from another process. 
	 */
	private void deliverTLC(byte[] data, byte source) {

		ByteBuffer buffer = ByteBuffer.wrap(data);
		int step = buffer.getInt();
		HashSet<Integer> value = new HashSet<Integer>();
		while(buffer.hasRemaining()) {
			value.add(buffer.getInt());
		}
		
		if (!storedTLCs.containsKey(step)) {
			storedTLCs.put(step, new LinkedList<TLC_Message>());
		}
		storedTLCs.get(step).add(new TLC_Message(source, step, value));
		
		if (step > currentStep) {
			return;
		}
	
		if (storedTLCs.get(step).size() > hosts.size()/2) {
			if (!decided[step]) {
				decide(allDecisions(step));
			}
			currentStep++;
			currentProposal++;
			
			// No need to keep all tlc messages after moving on to the next step, just the one that belongs to this node's decision. 
//			for(TLC_Message tlcMsg : storedTLCs.get(step)) {
//				if(tlcMsg.getSource() == (byte)myId) {
//					storedTLCs.put(step, new LinkedList<TLC_Message>() {
//						{
//						add(tlcMsg);
//						}
//					});
//					System.gc();
//					break;
//				}
//			}
			storedTLCs.get(step).removeIf(x -> x.getSource() != (byte)myId);

			propose(currentProposal);
		}
	}
	
	// Grab all decisions from TLC messages this node has for a certain step. Union of all decisions and the proposal of this node
	// will not violate the properties of Lattice Agreement. 
	private HashSet<Integer> allDecisions(int step) {
		
		HashSet<Integer> result = new HashSet<Integer>();
		synchronized(proposedValue) {
			result.addAll(proposedValue);
		}
		
		storedTLCs.get(step).forEach(x -> result.addAll(x.getValue()));
		
		return result;
	}
	
	public void decide(HashSet<Integer> decision) {

		int i = 0;
		int last = 0;
		for(Integer x : decision) {
			if (i == decision.size() - 1) {
				last = x;
				break;
			}
			try {
				bw.write(x + " ");
			}catch(IOException e) {
				e.printStackTrace();
			}
			i++;
		}
		try {
			bw.write(""+last);
			bw.write("\n");
		}catch(IOException e) {
			e.printStackTrace();
		}
		
		decided[currentStep] = true;
		broadcastTLC(currentStep, decision);
	}
	
	/*
	 * Broadcasts a TLC message from this process. 
	 */
	private void broadcastTLC(int step, HashSet<Integer> value) {
		
		msgId++;
		ByteBuffer buffer = ByteBuffer.allocate((value.size() + 1)*4);
		buffer.putInt(step);
		value.forEach(x -> buffer.putInt(x.intValue()));
		byte[] data = buffer.array();
	
//		if (!storedTLCs.containsKey(step)) {
//			storedTLCs.put(step, new LinkedList<TLC_Message>());
//		}
//		TLC_Message tlcMsg = new TLC_Message((byte)myId, step, value);
//		System.out.println("Stavljam moj TLC : " + tlcMsg);
//		storedTLCs.get(step).add(tlcMsg);
	
		// Sending TLC message to every host. 
		for(Host h : hosts) {
//			if (h.getId() != myId) {
			pl.send(msgId, (byte)myId, (byte)3, (byte)h.getId(), data);
//			}
		}
	}
	
	private void resetState() {
		
		synchronized(proposedValue) {
			proposedValue.clear();
		}
		synchronized(acceptedValue) {
			acceptedValue.clear();
		}
		ackCount = 0;
		nackCount = 0;
	}
	
	public void stopProposing() {
		pl.stopLinks();
	}
}
