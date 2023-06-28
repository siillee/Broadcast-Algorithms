package cs451;

import java.util.HashSet;

/*
 * Class implementing the Threshold Logical Clock (TLC) message. 
 */

public class TLC_Message {
	
	private byte source;
	private int step;
	private HashSet<Integer> value;
	
	public TLC_Message(byte source, int step, HashSet<Integer> value) {

		this.source = source;
		this.step = step;
		this.value = value;
	}
	
	public byte getSource() {
		return source;
	}
	
	public void setSource(byte source) {
		this.source = source;
	}

	public int getStep() {
		return step;
	}

	public void setStep(int step) {
		this.step = step;
	}

	public HashSet<Integer> getValue() {
		return value;
	}

	public void setValue(HashSet<Integer> value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "TLC_Message [source=" + source + ", step=" + step + ", value=" + value + "]";
	}
	
}
