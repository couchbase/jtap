package com.membase.jtap.internal;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.internal.Response;
import com.membase.jtap.message.HeaderMessage;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

public class SASLAuthenticator {
	private static final Logger LOG = LoggerFactory.getLogger(SASLAuthenticator.class);
	private static final String[] mechanisms = new String[] {"PLAIN"};
	
	private BlockingQueue<Response> wQueue;
	private SaslClient sasl;
	
	public SASLAuthenticator(String address, String protocol, String bucketname, String password,
			BlockingQueue<Response> wQueue) {
		this.wQueue = wQueue;
		try {
			sasl = Sasl.createSaslClient(mechanisms, null, protocol, address, null, 
					new PlainCallbackHandler(bucketname, password));
		} catch (SaslException e) {
			e.printStackTrace();
		}
	}
	
	public void handshake() {
		HeaderMessage request = new HeaderMessage();
		ByteBuffer bytes;
		request.setMagic(Magic.PROTOCOL_BINARY_REQ);
		request.setOpcode(Opcode.SASLLIST);
		request.printMessage();
		bytes = request.getBytes();
		wQueue.add(new Response(bytes, bytes.capacity()));
		
		while (!sasl.isComplete()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				//throw new SaslException("Sasl handshake interrupted before it could be completed");
				e.printStackTrace();
			}
		}
	}
	
	public void recieve(ResponseMessage message) {
		if (!sasl.isComplete()) {
			try {
				String response = message.getValue();
				byte[] res = (sasl.hasInitialResponse() ? sasl.evaluateChallenge(response.getBytes()) : null);
				RequestMessage saslRequest = new RequestMessage();
				saslRequest.setMagic(Magic.PROTOCOL_BINARY_REQ);
				saslRequest.setOpcode(Opcode.SASLAUTH);
				saslRequest.setName(response);
				saslRequest.setValue(new String(res));
				
				ByteBuffer bytes = saslRequest.getBytes();
				wQueue.add(new Response(bytes, bytes.capacity()));
			} catch (SaslException e) {
				LOG.error("SASL Authentication Failed");
				System.exit(1);
			}
		} else {
			LOG.info("SASL Authentication completed");
		}
	}
}
