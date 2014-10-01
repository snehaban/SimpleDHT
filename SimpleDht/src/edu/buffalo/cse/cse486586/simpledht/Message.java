package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

public class Message implements Serializable {

	private static final long serialVersionUID = 1L;

	public static enum MessageType {
		JOIN("JOIN"), UPDATE_NEIGHBOURS("UPDATE_NEIGHBOURS"), 
		INSERT("INSERT"), INSERT_ACK("INSERT_ACK"),
		QUERY("QUERY"), QUERY_GLOBAL("QUERY_GLOBAL"), QUERY_REPONSE("QUERY_REPONSE"), 
		DELETE("DELETE"), DELETE_GLOBAL("DELETE_GLOBAL"), DELETE_ACK("DELETE_ACK");

		private final String type;

		private MessageType(String s) {
			type = s;
		}

		public boolean equalsName(String otherType) {
			return (otherType == null) ? false : type.equals(otherType);
		}

		public String toString() {
			return type;
		}
	};

	private MessageType msgType;
	private String requestingPort;
	private String successorPort;
	private String predecessorPort;
	private String forwardingPort;
	private String key; // for insert/query
	private String value; // for insert/query
	private String queryParam;
	private HashMap<String, String> allEntries;

	public MessageType getMsgType() {
		return msgType;
	}

	public void setMsgType(MessageType msgType) {
		this.msgType = msgType;
	}

	public String getRequestingPort() {
		return requestingPort;
	}

	public void setRequestingPort(String requestingPort) {
		this.requestingPort = requestingPort;
	}

	public String getPredecessorPort() {
		return predecessorPort;
	}

	public void setPredecessorPort(String predecessorPort) {
		this.predecessorPort = predecessorPort;
	}

	public String getSuccessorPort() {
		return successorPort;
	}

	public void setSuccessorPort(String sPort) {
		successorPort = sPort;
	}

	public String getForwardingPort() {
		return forwardingPort;
	}

	public void setForwardingPort(String forwardingPort) {
		this.forwardingPort = forwardingPort;
	}

	public String getQueryParam() {
		return queryParam;
	}

	public void setQueryParam(String queryParam) {
		this.queryParam = queryParam;
	}

	public HashMap<String, String> getAllEntries() {
		return allEntries;
	}

	public void setAllEntries(HashMap<String, String> allEntries) {
		this.allEntries = allEntries;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
