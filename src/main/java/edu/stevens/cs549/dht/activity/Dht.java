package edu.stevens.cs549.dht.activity;

import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.main.WebClient;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.state.IRouting;
import edu.stevens.cs549.dht.state.IState;
import edu.stevens.cs549.dht.state.State;
import java.util.logging.Logger;

public class Dht extends DhtBase implements IDhtService, IDhtNode, IDhtBackground {

	/*
	 * DHT logic.
	 * 
	 * This logic may be invoked from a RESTful Web service or from the command
	 * line, as reflected by the interfaces.
	 */

	/*
	 * Client stub for Web service calls.
	 */
	protected WebClient client = new WebClient();

	public WebClient getClient() {
		return client;
	}

	/*
	 * Logging operations.
	 */

	protected static final String TAG = Dht.class.getCanonicalName();
	
	private Logger log = Logger.getLogger(TAG);

	private void info(String s) {
		log.info(s);
	}

	private void warning(String s) {
		log.warning(s);
	}

	private void severe(String s) {
		log.severe(s);
	}

	/*
	 * The URL for this node in the DHT. Although the info is stored in state,
	 * we should be able to cache it locally since it does not change.
	 */
	protected NodeInfo info;

	/*
	 * Remote clients may call this when joining the network and they know only
	 * our URI and need the node identifier (key) as well.
	 */
	// WebMethod
	public NodeInfo getNodeInfo() {
		assert this.info != null;
		return this.info;
	}

	/*
	 * Key-value pairs stored in this node.
	 */
	private IState state;

	/*
	 * Finger tables, and predecessor and successor pointers.
	 */
	private IRouting routing;

	/*
	 * This constructor is called when a DHT object is created for the CLI.
	 */

	/*
	 * This constructor is called when a DHT object is created in a Web service.
	 */
	private Dht() {
		this.state = State.getState();
		this.routing = (IRouting) state;
		this.info = state.getNodeInfo();
	}

	public static Dht getDht() {
		return new Dht();
	}

	/*
	 * Get the successor of a node. Need to perform a Web service call to that
	 * node, and it then retrieves its routing state from its local RMI object.
	 * 
	 * Make a special case for when this is the local node, i.e.,
	 * info.addr.equals(localInfo.addr), otherwise get an infinite loop.
	 */
	private NodeInfo getSucc(NodeInfo info) throws Failed {
		NodeInfo localInfo = this.getNodeInfo();
		if (localInfo.getHost().equals(info.getHost()) && localInfo.getPort()==info.getPort()) {
			return getSucc();
		} else {
			// TODO: Do the Web service call
			return client.getSucc(info);

		}
	}

	/*
	 * This version gets the local successor from routing tables.
	 */
	// WebMethod
	public NodeInfo getSucc() {
		return routing.getSucc();
	}

	/*
	 * Set the local successor pointer in the routing tables.
	 */
	public void setSucc(NodeInfo succ) {
		routing.setSucc(succ);
	}

	/*
	 * Get the predecessor of a node. Need to perform a Web service call to that
	 * node, and it then retrieves its routing state from its local routing
	 * tables.
	 * 
	 * Make a special case for when this is the local node, i.e.,
	 * info.addr.equals(localInfo.addr), otherwise get an infinite loop.
	 */
	protected NodeInfo getPred(NodeInfo info) throws Failed {
		NodeInfo localInfo = this.getNodeInfo();
		if (localInfo.getHost().equals(info.getHost()) && localInfo.getPort()==info.getPort()) {
			return getPred();
		} else {
			/*
			 * TODO: Do the Web service call
			 */
			return client.getPred(info);

		}
	}

	/*
	 * This version gets the local predecessor from routing tabes.
	 */
	// WebMethod
	public NodeInfo getPred() {
		return routing.getPred();
	}

	/*
	 * Set the local predecessor pointer in the routing tables.
	 */
	protected void setPred(NodeInfo pred) {
		routing.setPred(pred);
	}

	/*
	 * Perform a Web service call to get the closest preceding finger in the
	 * finger table of the argument node.
	 */
	protected NodeInfo closestPrecedingFinger(NodeInfo info, int id) throws Failed {
		NodeInfo localInfo = this.getNodeInfo();
		if (localInfo.equals(info)) {
			return closestPrecedingFinger(id);
		} else {
			if (IRouting.USE_FINGER_TABLE) {
				/*
				 * TODO: Do the Web service call to the remote node.
				 */
				return client.closestPrecedingFinger(info,id);
			} else {
				/*
				 * Without finger tables, just use the successor pointer.
				 */
				return getSucc(info);
			}
		}
	}

	/*
	 * For the local version, get from the routing table.
	 */
	// WebMethod
	public NodeInfo closestPrecedingFinger(int id) {
		return routing.closestPrecedingFinger(id);
	}

	/*
	 * Set a finger pointer.
	 */
	protected void setFinger(int ix, NodeInfo node) {
		routing.setFinger(ix, node);
	}

	/*
	 * Find the node that will hold bindings for a key k. Search the circular
	 * list to find the node. Stop when the node's successor stores values
	 * greater than k.
	 */
	protected NodeInfo findPredecessor(int id) throws Failed {
		NodeInfo info = getNodeInfo();
		NodeInfo succ = getSucc(info);
		if (info.getId() != succ.getId()) {
			while (!inInterval(id, info.getId(), succ.getId())) {
				info = closestPrecedingFinger(info, id);
				succ = getSucc(info);
				/*
				 * Break out of infinite loop (e.g. our successor may be
				 * a single-node network that still points to itself).
				 */
				if (getNodeInfo().equals(info)) {
					break;
				}
			}
		}
		return info;
	}

	/*
	 * Find the successor of k, starting at the current node. Called internally
	 * and (server-side) in join protocol.
	 */
	// WebMethod
	public NodeInfo findSuccessor(int id) throws Failed {
		NodeInfo predInfo = findPredecessor(id);
		return getSucc(predInfo);
	}

	/*
	 * Stabilization logic.
	 * 
	 * Called by background thread & in join protocol by joining node (as new
	 * predecessor).
	 */
	public boolean stabilize() throws Failed {
		Log.background(TAG, "Starting stabilize()");
		NodeInfo info = getNodeInfo();
		NodeInfo succ = getSucc();
		if (info.equals(succ)) {
			Log.background(TAG, "Ending stabilize() 1");
			return true;
		}

		// Possible synchronization point (see join() below).
		NodeInfo predOfSucc = getPred(succ);
		if (predOfSucc == null || predOfSucc.getId() == 0) {
			/*
			 * Successor's predecessor is not set, so we will become pred.
			 * Notify succ that we believe we are its predecessor. Provide our
			 * bindings, that will be backed up by our successor. Expect
			 * transfer of bindings from succ as ack.
			 * 
			 * Note: We pass in our bindings to the successor for backing up.
			 * We don't use this now, but might in a future assignment.
			 * 
			 * Do the Web service call.
			 */
			Log.debug(TAG, "Joining succ with null pred.");
			NodeBindings db = client.notify(succ, state.extractBindings());
			Log.background(TAG, "Ending stabilize() 2");
			return notifyContinue(db);
		} else if (inInterval(predOfSucc.getId(), info.getId(), succ.getId(), false)) {
			setSucc(predOfSucc);
			/*
			 * Successor's predecessor should be our predecessor. Notify pred of
			 * succ that we believe we are its predecessor. Expect transfer of
			 * bindings from succ as ack.
			 * 
			 * Do the Web service call.
			 */
			Log.debug(TAG, "Joining succ as new, closer pred.");
			/*
			 * Note: We notify predOfSucc in this case, since we have
			 * set that as our successor.
			 */
			NodeBindings db = client.notify(predOfSucc, state.extractBindings());
			Log.background(TAG, "Ending stabilize() 3");
			// The bindings we got back will be merged with our current bindings.
			return notifyContinue(db);
		} else if (inInterval(info.getId(), predOfSucc.getId(), succ.getId(), false)) {
			/*
			 * Has some node inserted itself between us and the successor? This
			 * could happen due to a race condition between setting our
			 * successor pointer and notifying the successor.
			 */
			Log.debug(TAG, "Notifying succ that we are its pred.");
			NodeBindings db = client.notify(succ, state.extractBindings());
			Log.background(TAG, "Ending stabilize() 4");
			return notifyContinue(db);
		} else {
			/*
			 * We come here if we are already the predecessor, so no change.
			 */
			Log.background(TAG, "Ending stabilize() 5");
			return false;
		}
	}

	// WebMethod
	public NodeBindings notify(NodeBindings predDb) {
		/*
		 * Node cand ("candidate") believes it is our predecessor.
		 */
		NodeInfo cand = predDb.getInfo();
		NodeInfo pred = getPred();
		NodeInfo info = getNodeInfo();
		if (pred == null || inInterval(cand.getId(), pred.getId(), info.getId(), false)) {
			setPred(cand);
			/*
			 * If this is currently a single-node network, close the loop by
			 * setting our successor to our pred. Thereafter the network should
			 * update automatically as new predecessors are detected by old
			 * predecessors.
			 */
			if (getSucc().equals(info)) {
				setSucc(cand);
				// We must also set pred of succ (i.e. cand) to point back
				// to us. This will be done by stabilize().
			}
			/*
			 * Transfer our bindings up to cand.id to our new pred (the
			 * transferred bindings will be in the response). We will back up
			 * their bindings (this was sent as an argument in notify()).
			 */
			NodeBindings db = transferBindings(cand.getId());
			// Back up predecessor bindings. Might be used in future assignment.
			state.backupBindings(predDb);
			Log.debug(TAG, "Transferring bindings to node id=" + cand.getId());
			return db;
		} else {
			/*
			 * Null indicates that we did not accept new pred. This may be
			 * because cand==pred.
			 */
			return null;
		}
	}

	/*
	 * Process the result of a notify to a potential successor node.
	 */
	protected boolean notifyContinue(NodeBindings db) {
		if (db == null) {
			/*
			 * We are out.
			 */
			return false;
		} else {
			/*
			 * Set pred pointer for the case where our successor is also our
			 * predecessor?
			 */
			if (getNodeInfo().equals(db.getSucc())) {
				setPred(db.getInfo());
			}
			/*
			 * db is bindings we take from the successor.
			 */
			Log.debug(TAG, "Installing bindings from successor.");
			state.installBindings(db);
			return true;
		}
	}

	/*
	 * Transfer our bindings up to predId to a new predecessor.
	 */
	protected NodeBindings transferBindings(int predId) {
		NodeBindings db = state.extractBindings(predId);
		state.dropBindings(predId);
		return db;
	}

	private int next = 0;

	/*
	 * Periodic refresh of finger table based on changed successor.
	 */
	protected void fixFinger() {
		int localNext;
		synchronized(state) {
			next = (next + 1) % IRouting.NFINGERS;
			localNext = next;
		}
		/*
		 * Compute offset = 2^next
		 */
		int offset = 1;
		for (int i = 0; i < localNext; i++)
			offset = offset * 2;
		/*
		 * finger[next] = findSuccessor (n + 2^next)
		 */
		int nextId = (getNodeInfo().getId() + offset) % IRouting.NKEYS;
		try {
			setFinger(localNext, findSuccessor(nextId));
		} catch (Failed e) {
			warning("FixFinger: findSuccessor(" + nextId + ") failed.");
		}
	}

	/*
	 * Speed up the finger table refresh.
	 */
	// Called by background thread.
	public void fixFingers(int ntimes) throws java.lang.Error {
		for (int i = 0; i < ntimes; i++) {
			fixFinger();
		}
	}

	/*
	 * Check if the predecessor has failed.
	 */
	// Called by background thread.
	public void checkPredecessor() {
		/*
		 * Ping the predecessor node by asking for its successor.
		 */
		NodeInfo pred = getPred();
		if (pred != null) {
			try {
				getSucc(pred);
			} catch (Failed e) {
				info("CheckPredecessor: Predecessor has failed (id=" + pred.getId() + ")");
				setPred(null);
			}
		}
	}

	/*
	 * Get the values under a key at the specified node. If the node is the
	 * current one, go to the local state.
	 */
	protected String[] get(NodeInfo n, String k) throws Failed {
		if (n.getHost().equals(info.getHost()) && n.getPort()==info.getPort()) {
			try {
				return this.get(k);
			} catch (Invalid e) {
				severe("Get: invalid internal inputs: " + e);
				throw new IllegalArgumentException(e);
			}
		} else {
			/*
			 * Retrieve the bindings at the specified node.
			 * 
			 * TODO: Do the Web service call.
			 */
			return client.get(n,k);
		}
	}

	/*
	 * Retrieve values under the key at the current node.
	 */
	// WebMethod
	public String[] get(String k) throws Invalid {
		return state.get(k);
	}

	/*
	 * Add a value under a key.
	 */
	public void add(NodeInfo n, String k, String v) throws Failed {
		if (n.getHost().equals(info.getHost()) && n.getPort()==info.getPort()) {
			try {
				add(k, v);
			} catch (Invalid e) {
				severe("Add: invalid internal inputs: " + e);
				throw new IllegalArgumentException(e);
			}
		} else {
			/*
			 * TODO: Do the Web service call.
			 */
			client.add(n, k, v);

		}
	}

	/*
	 * Store a value under a key at the local node.
	 */
	// WebMethod
	public void add(String k, String v) throws Invalid {
		/*
		 * Validate that this binding can be stored here.
		 */
		int kid = NodeKey(k);
		NodeInfo info = getNodeInfo();
		NodeInfo pred = getPred();

		if (pred != null && inInterval(kid, pred.getId(), info.getId(), true)) {
			/*
			 * This node covers the interval in which k should be stored.
			 */
			state.add(k, v);
		} else if (pred == null && info.equals(getSucc())) {
			/*
			 * Single-node network.
			 */
			state.add(k, v);
		} else if (info.getId() == kid) {
			/*
			 * ???
			 */
			state.add(k, v);			
		} else if (pred == null && info.equals(getSucc())) {
			severe("Add: predecessor is null but not a single-node network.");
		} else {
			throw new Invalid("Invalid key: " + k + " (id=" + kid + ")");
		}
	}

	/*
	 * Delete value under a key.
	 */
	public void delete(NodeInfo n, String k, String v) throws Failed {
		if (n.getHost().equals(info.getHost()) && n.getPort()==info.getPort()) {
			try {
				delete(k, v);
			} catch (Invalid e) {
				severe("Delete: invalid internal inputs: " + e);
				throw new IllegalArgumentException(e);
			}
		} else {
			/*
			 * TODO: Do the Web service call.
			 */
			client.delete(n,k,v);

		}
	}

	/*
	 * Delete value under a key at the local node.
	 */
	// WebMethod
	public void delete(String k, String v) throws Invalid {
		state.delete(k, v);
	}

	/*
	 * These operations perform the CRUD logic at the network level.
	 */

	/*
	 * Store a value under a key in the network.
	 */
	public void addNet(String skey, String v) throws Failed {
		int id = NodeKey(skey);
		//if the key's id is equal to the nodes id then add it locally
		if (info.getId() == id)
			add(info, skey, v);
		else {
			NodeInfo succ = this.findSuccessor(id);
			add(succ, skey, v);
		}
	}

	/*
	 * Get the values under a key in the network.
	 */
	public String[] getNet(String skey) throws Failed {
		int id = NodeKey(skey);
		// if the key's id is equal to the nodes id then get it locally
		if (info.getId() == id)
			return get(info, skey);
		else {
			NodeInfo succ = this.findSuccessor(id);
			return get(succ, skey);
		}
	}

	/*
	 * Delete a value under a key in the network.
	 */
	public void deleteNet(String skey, String v) throws Failed {
		int id = NodeKey(skey);
		//if the key's id is equal to the nodes id then delete it locally
		if (info.getId() == id)
			delete(info, skey, v);
		else {
			NodeInfo succ = this.findSuccessor(id);
			delete(succ, skey, v);
		}
	}

	/*
	 * Join logic.
	 */

	/*
	 * Insert this node into the DHT identified by addr.
	 */
	public void join(String host, int port) throws Failed, Invalid {
		System.out.println("Host: " + host + ", Port: " + port);
		setPred(null);
		// Clear local bindings
		state.clear();
		NodeInfo info = getNodeInfo();
		NodeInfo succ;

		// Find successor using WebClient
		succ = client.findSuccessor(NodeInfo.newBuilder().setHost(host).setPort(port).build(), info.getId());

		// Set the found successor
		setSucc(succ);

		// Run stabilize logic
		stabilize();


	}

	/*
	 * State display operations for CLI.
	 */

	public void display() {
		state.display();
	}

	public void routes() {
		routing.routes();
	}
	

}
