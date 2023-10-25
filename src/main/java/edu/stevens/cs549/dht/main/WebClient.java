package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc;
import edu.stevens.cs549.dht.rpc.Id;
import edu.stevens.cs549.dht.rpc.Key;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	/*
	 * Encapsulate Web client operations here.
	 * 
	 * TODO: Fill in missing operations.
	 */



	public NodeInfo getSucc(NodeInfo node) {
		return call(node, stub -> {
			// Call the remote getSucc method
			return stub.getSucc(Empty.newBuilder().build());
		});
	}


	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) {
		return call(node, stub -> {
			// Create the Id object with the given id
			Id requestId = Id.newBuilder().setId(id).build();

			// Call the remote closestPrecedingFinger method
			return stub.closestPrecedingFinger(requestId);
		});
	}


// Implement methods for getBindings, addBinding, deleteBinding, and findSuccessor

	public String[] get(NodeInfo node, String key) {
		return call(node, (stub) -> {
			// Create the request object using the Key message from your proto
			Key request = Key.newBuilder()
					.setKey(key)
					.build();

			// Call the remote getBindings method
			Bindings response = stub.getBindings(request);

			// Extract the string array (values) from the response
			return response.getValueList().toArray(new String[0]);
		});
	}

	public void add(NodeInfo node, String key, String value) {
		call(node, (stub) -> {
			// Create the request object using the Binding message from your proto
			Binding request = Binding.newBuilder()
					.setKey(key)
					.setValue(value)
					.build();

			// Call the remote addBinding method
			stub.addBinding(request);

			// As the add operation typically doesn't return a value,
			// you can simply return null or an acknowledgment message if needed.
			return null;
		});
	}


	public void delete(NodeInfo node, String key, String value) {
		call(node, (stub) -> {
			// Create the request object using the Binding message from your proto
			Binding request = Binding.newBuilder()
					.setKey(key)
					.setValue(value)
					.build();

			// Call the remote deleteBinding method
			stub.deleteBinding(request);

			// As the delete operation typically doesn't return a value,
			// you can simply return null or an acknowledgment message if needed.
			return null;
		});
	}


	public NodeInfo findSuccessor(NodeInfo node, int id) {
		return call(node, stub -> {
			// Create the Id object with the given id
			Id requestId = Id.newBuilder().setId(id).build();

			// Call the remote findSuccessor method
			return stub.findSuccessor(requestId);
		});
	}





	/*
	 * A generic interface for a web service call, once a stub is obtained.
	 */
	public interface ClientCall<T> {
		public T execute(DhtServiceGrpc.DhtServiceBlockingStub stub);
	}

	/*
	 * Wrap a service call with the boilerplate logic for creating the channel and stub,
	 * then shutting down the channel when the call is complete.
	 */
	private <T> T call(String targetHost, int targetPort, ClientCall<T> client) {
		ChannelCredentials credentials = InsecureChannelCredentials.create();
		ManagedChannel channel = Grpc.newChannelBuilderForAddress(targetHost, targetPort, credentials).build();
		try {
			return client.execute(DhtServiceGrpc.newBlockingStub(channel));
		} finally {
			channel.shutdown();
		}
	}

	private <T> T call(NodeInfo target, ClientCall<T> client) {
		if (target.getHost() == null || target.getHost().trim().isEmpty() || target.getPort() <= 0) {
			throw new IllegalArgumentException("Invalid NodeInfo: Host = " + target.getHost() + ", Port = " + target.getPort());
		}
		return call(target.getHost(), target.getPort(), client);
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}


	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getPred(NodeInfo node) {
		return call(node, stub -> {
			// Call the remote getPred method
			return stub.getPred(Empty.newBuilder().build());
		});
	}

	public NodeBindings notify(NodeInfo targetNode, NodeBindings predDb) {
		return call(targetNode, stub -> {
			// Call the remote notify method on the given targetNode
			return stub.notify(predDb);
		});
	}

}
