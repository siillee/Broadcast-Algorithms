package cs451;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class Message implements Serializable, Comparable<Message> {

	private int msgId;
	private byte source;
//	private byte relay;
	// Type of message : 0 == proposal, 1 == ACK, 2 == NACK, 3 == TLC 
	private byte type;
	private byte destination;
	private byte[] data;
	private boolean ack;
	
	public int numOfRetransmits;
	public long time;

	public Message(int msgId, byte source, byte type, byte destination, byte[] data, boolean ack) {
		
		this.msgId = msgId;
		this.source = source;
//		this.relay = relay;
		this.type = type;
		this.destination = destination;
		this.data = data;
		this.ack = ack;
		
		this.numOfRetransmits = 0;
		this.time = System.currentTimeMillis();
	}
	
	public int getMsgId() {
		return msgId;
	}

	public void setMsgId(int msgId) {
		this.msgId = msgId;
	}

	public byte getSource() {
		return source;
	}

	public void setSource(byte source) {
		this.source = source;
	}

//	public byte getRelay() {
//		return this.relay;
//	}
//	
//	public void setRelay(byte relay) {
//		this.relay = relay;
//	}
	
	public byte getType() {
		return this.type;
	}
	
	public void setType(byte type) {
		this.type = type;
	}
	
	public byte getDestination() {
		return destination;
	}

	public void setDestination(byte destination) {
		this.destination = destination;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public boolean isAck() {
		return ack;
	}

	public void setAck(boolean ack) {
		this.ack = ack;
	}

	@Override
	public int hashCode() {
		return Objects.hash(msgId, source);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		return msgId == other.msgId && source == other.source;
	}

	@Override
	public int compareTo(Message o) {
		
		int tmp = Integer.compare(this.source, o.source);
		if (tmp != 0) {
			return tmp;
		}
		
		return Integer.compare(this.msgId, o.msgId);
//		int tmp2 = Integer.compare(this.msgId, o.msgId);
//		if (tmp2 != 0) {
//			return tmp2;
//		}
		
//		return Integer.compare(this.relay, o.relay);
	}
	
	@Override
	public String toString() {
		return "Message [messageId=" + msgId + ", sourceId=" + source + ", type=" + type + ", destinationId="
				+ destination + ", data=" + Arrays.toString(data) + ", ack=" + ack + ", numOfRetransmits="
				+ numOfRetransmits + ", time=" + time + "]";
	}
	
	
}
