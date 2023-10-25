package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceImplBase;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService extends DhtServiceImplBase {
	
	private static final String TAG = NodeService.class.getCanonicalName();
	
	private static Logger logger = Logger.getLogger(TAG);

	/**
	 * Each service request is processed by a distinct service object.
	 *
	 * Shared state is in the state object; we use the singleton pattern to make sure it is shared.
	 */
	private Dht getDht() {
		return Dht.getDht();
	}
	
	// TODO: add the missing operations

	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

	@Override
	public void getNodeInfo(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getNodeInfo()");
		responseObserver.onNext(getDht().getNodeInfo());
		responseObserver.onCompleted();
	}



	@Override
	public void getSucc(Empty request, StreamObserver<NodeInfo> responseObserver) {
		try {
			// Get the successor using the DHT's local method
			NodeInfo successor = getDht().getSucc();

			// Return the found NodeInfo as a response
			responseObserver.onNext(successor);
			responseObserver.onCompleted();
		} catch (Exception e) {
			error("Unexpected error while processing getSucc request.", e);
			responseObserver.onError(e); // Or use more specific error handling logic
		}
	}


	@Override
	public void getPred(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		try {
			// Get the predecessor using the DHT's local method
			NodeInfo predecessor = getDht().getPred();

			// Return the found NodeInfo as a response
			responseObserver.onNext(predecessor);
			responseObserver.onCompleted();
		} catch (Exception e) {
			error("Unexpected error while processing getPred request.", e);
			responseObserver.onError(e); // Or use more specific error handling logic
		}
	}

	@Override
	public void closestPrecedingFinger(Id request, StreamObserver<NodeInfo> responseObserver) {
		try {
			// Get the closest preceding finger for the given id using the DHT's method
			NodeInfo closestPrecedingNode = getDht().closestPrecedingFinger(request.getId());

			// Return the found NodeInfo as a response
			responseObserver.onNext(closestPrecedingNode);
			responseObserver.onCompleted();
		} catch (Exception e) {
			error("Unexpected error while processing closestPrecedingFinger request.", e);
			responseObserver.onError(e); // Or a more specific error handling logic
		}
	}


	@Override
	public void notify(NodeBindings request, StreamObserver<NodeBindings> responseObserver) {
		try {
			// Handle the notify request using the DHT's local method
			NodeBindings responseBindings = getDht().notify(request);

			// Return the found NodeBindings as a response
			responseObserver.onNext(responseBindings);
			responseObserver.onCompleted();
		} catch (Exception e) {
			error("Unexpected error while processing notify request.", e);
			responseObserver.onError(e); // Or use more specific error handling logic
		}
	}

	@Override
	public void getBindings(Key request, StreamObserver<Bindings> responseObserver) {
		Log.weblog(TAG, "getBindings()");
		try {
			// Retrieve the values from the DHT service using the provided key
			String[] values = getDht().get(request.getKey());

			// Create the response object using the Bindings message from your proto
			Bindings.Builder responseBuilder = Bindings.newBuilder()
					.setKey(request.getKey());
			for (String value : values) {
				responseBuilder.addValue(value);
			}

			// Send the response to the client
			responseObserver.onNext(responseBuilder.build());
			responseObserver.onCompleted();
		} catch (Invalid e) {
			error("Invalid key provided.", e);
			responseObserver.onError(e); // Or a more specific error handling logic
		} catch (Exception e) {
			error("Unexpected error while processing getBindings request.", e);
			responseObserver.onError(e); // Or a more specific error handling logic
		}
	}


	@Override
	public void addBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "addBinding()");

		try {
			// Extract key and value from the request and add it to the DHT service
			getDht().add(request.getKey(), request.getValue());

			// Respond with an empty acknowledgment
			responseObserver.onNext(Empty.newBuilder().build());
			responseObserver.onCompleted();
		} catch (Invalid e) {
			error("Invalid binding provided.", e);
			responseObserver.onError(e); // Or use a more specific error handling logic
		} catch (Exception e) {
			error("Unexpected error while processing addBinding request.", e);
			responseObserver.onError(e); // Or use a more specific error handling logic
		}
	}


	@Override
	public void deleteBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "deleteBinding()");
		try {
			// Delete the binding from the DHT service using the provided key and value
			getDht().delete(request.getKey(), request.getValue());

			// If the delete operation succeeds, simply return an Empty response as acknowledgment
			responseObserver.onNext(Empty.newBuilder().build());
			responseObserver.onCompleted();
		} catch (Invalid e) {
			error("Invalid key or value provided.", e);
			responseObserver.onError(e); // Or a more specific error handling logic
		} catch (Exception e) {
			error("Unexpected error while processing deleteBinding request.", e);
			responseObserver.onError(e); // Or a more specific error handling logic
		}
	}


	@Override
	public void findSuccessor(Id request, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "findSuccessor()");
		try {
			// Get the successor node info for the given id using the DHT's findSuccessor method
			NodeInfo successorInfo = getDht().findSuccessor(request.getId());

			if (successorInfo.getHost() == null || successorInfo.getHost().trim().isEmpty() || successorInfo.getPort() <= 0) {
				throw new IllegalArgumentException("Invalid NodeInfo: Host = " + successorInfo.getHost() + ", Port = " + successorInfo.getPort());
			}

			// Return the found NodeInfo as a response
			responseObserver.onNext(successorInfo);
			responseObserver.onCompleted();
		} catch (Failed e) {
			error("Failed to find successor.", e);
			responseObserver.onError(e); // Or a more specific error handling logic
		} catch (Exception e) {
			error("Unexpected error while processing findSuccessor request.", e);
			responseObserver.onError(e); // Or a more specific error handling logic
		}
	}



}