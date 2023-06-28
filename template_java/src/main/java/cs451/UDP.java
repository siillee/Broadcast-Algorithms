package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UDP {
	// The size of the buffer, 65000, as it generally is in UDP. 
	private static final int BUFFER_SIZE = 65000;

	private DatagramSocket socket;
	private DatagramPacket inputDatagram;
	private DatagramPacket[] outputDatagrams;
	private PerfectLinks pl;
	private BlockingQueue<Message> sendQueue;
	private ByteBuffer input;
	private ByteBuffer output;
	private volatile boolean close;
	private Thread sender;
	private Thread receiver;
	
	public UDP(String ip, int port, List<Host> hosts, PerfectLinks pl) {
		
		try {
			this.socket = new DatagramSocket(port);
		}catch (SocketException e) {
			System.out.println("Error when initializing socket ");
			e.printStackTrace();
		}
		
		this.pl = pl;
		
		inputDatagram = new DatagramPacket(new byte[BUFFER_SIZE], BUFFER_SIZE, new InetSocketAddress(ip, port));
		input = ByteBuffer.wrap(inputDatagram.getData(), 0, BUFFER_SIZE);
		
		output = ByteBuffer.allocate(BUFFER_SIZE);
		outputDatagrams = new DatagramPacket[hosts.size()];
		for (Host h : hosts) {
			outputDatagrams[h.getId()-1] = new DatagramPacket(output.array(), 0, new InetSocketAddress(h.getIp(), h.getPort()));
		}
		
		sendQueue = new LinkedBlockingQueue<>();
		
		startSendThread();
		startReceiveThread();
	}
	
	public void send(Message mess) {
		sendQueue.add(mess);
	}
	
	private void startSendThread() {
		
		sender = new Thread(() -> {
			while(!close) {
				try {
					Message mess = sendQueue.take();
					
					if (close) {
						return;
					}
				
					output.clear();
					output.putInt(mess.getMsgId());
					output.put((byte) mess.getSource());
					output.put((byte) mess.getType());
					output.put((byte) mess.getDestination());
					output.put((byte) (mess.isAck() ? 1 : 0));
					output.put(mess.getData());
					
					// Check if message is an ack 
					// and return it to the source (i.e. the node that sent the message for which the ack is sent).
					int destId =  mess.getDestination();
					if (mess.isAck()) {
						destId = mess.getSource();
					}
					DatagramPacket packet = outputDatagrams[destId - 1];
					packet.setData(output.array(), 0, output.position());
					socket.send(packet);
					
				}catch (Exception e){
					e.printStackTrace();
					return;
				}
			}
		});
		
		sender.start();
	}
	
	private void startReceiveThread() {
		
		receiver = new Thread(() -> {
			while (!close) {
				try {
					socket.receive(inputDatagram);
					
					if (close) {
						return;
					}
					
					input = ByteBuffer.wrap(inputDatagram.getData(), inputDatagram.getOffset(), inputDatagram.getLength());
					input.clear();

					int messageId = input.getInt();
					byte source = input.get();
					byte type = input.get();
					byte destination = input.get();
					boolean ack = input.get() == (byte) 1;
					byte[] data = Arrays.copyOfRange(input.array(), 8, inputDatagram.getLength());

					pl.deliver(new Message(messageId, source, type, destination, data, ack));
					
				}catch (Exception e) {
					e.printStackTrace();
					return;
				}
			}
		});

		receiver.start();
	}
	
	public void closeSocket() {

        socket.close();
        close = true;
        
        try {
            receiver.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        try {
       	 // Adding an empty message in order for the socket to not block forever.
           sendQueue.add(new Message(0, (byte)0, (byte)0, (byte)0, null, false));
           sender.join();
       } catch (InterruptedException e) {
           e.printStackTrace();
       }
	}
	
}
