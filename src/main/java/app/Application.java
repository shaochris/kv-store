package app;

import java.awt.image.RescaleOp;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.HttpServletRequest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

@SpringBootApplication
@RestController
@EnableScheduling
public class Application {


static Replica replica = null;


// PUT Operation
@ResponseBody
@RequestMapping(value = "/key-value-store/{key}", method = RequestMethod.PUT, produces = "application/json")
public ResponseEntity<String> putKey(@PathVariable String key, @RequestBody Map<String, Object> jsonData, HttpServletRequest request) {
	Response resp = null;
	URL sender_url = appURL.processURL(request.getRemoteAddr() + ":" + 8080);

	// make sure data includes all required fields
	for (String required: new String[] {"causal-metadata", "value"}) {
		if (!jsonData.keySet().contains(required)) {
			Response missingData = Response.createJsonResponse(
				400,
				Response.pair("error", String.format("%s is missing", (required.equals("value")) ? "Value" : "Causal Metadata")),
				Response.pair("message", "Error in PUT")
				);
			return new ResponseEntity<String>(missingData.responseString, missingData.httpCode);
		}
	}
	System.out.println("=======================");
	System.out.println("key");
	System.out.println(jsonData.get("value"));
	System.out.println(jsonData.get("causal-metadata"));
	System.out.println(sender_url);
	System.out.println(jsonData.toString());
	System.out.println("=======================");
	// broadcast is sent in putKey
	int hash = Hash.hash32(key.getBytes());
	int target_shard = Hash.unsignedReminaderPlusOne(hash, replica.shard_count);
	System.out.println("REPLICA SHARD ID " + replica.shard_id + " REPLICA SHARD COUNT " + replica.shard_count + " HASH: " + hash +" TARGET SHARD: " + target_shard);
	if (replica.shard_id != target_shard) {
		resp = replica.forwardShardPUT(target_shard, key, jsonData, sender_url);
	} else {
		resp = replica.putKey(key, jsonData.get("value"), String.valueOf(jsonData.get("causal-metadata")), jsonData, sender_url);
	}

	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}

// GET Operation
@RequestMapping(value = "/key-value-store/{key}", method = RequestMethod.GET, produces = "application/json")
public ResponseEntity<String> getValue(@PathVariable String key) {

	// determine which shard to send to
	int hash = Hash.hash32(key.getBytes());
	int target_shard = Hash.unsignedReminaderPlusOne(hash, replica.shard_count);
	if (replica.shard_id.equals(target_shard)) {
		// key is on this shard
		Response resp = this.replica.kvStore.get(key);
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	} else {
		// send to shard that corresponds with target
		Response resp = this.replica.forwardShardGET(target_shard, key);
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	}
}

// DELETE Operation
@RequestMapping(value = "/key-value-store/{key}", method = RequestMethod.DELETE, produces = "application/json")
public ResponseEntity<String> deleteValue(@PathVariable String key, @RequestBody Map<String, Object> jsonData, HttpServletRequest request) {
	Response resp = null;
	URL sender_url = appURL.processURL(request.getRemoteAddr() + ":" + 8080);

	// make sure data includes all required fields
	for (String required: new String[] {"causal-metadata", "value"}) {
		if (!jsonData.keySet().contains(required)) {
			Response missingData = Response.createJsonResponse(
				400,
				Response.pair("error", String.format("%s is missing", (required.equals("value")) ? "Value" : "Causal Metadata")),
				Response.pair("message", "Error in PUT")
				);
			return new ResponseEntity<String>(missingData.responseString, missingData.httpCode);
		}
	}
	System.out.println("=======================");
	System.out.println("key");
	System.out.println(jsonData.get("value"));
	System.out.println(jsonData.get("causal-metadata"));
	System.out.println(sender_url);
	System.out.println(jsonData.toString());
	System.out.println("=======================");
	// broadcast is sent in putKey
	int hash = Hash.hash32(key.getBytes());
	int target_shard = Hash.unsignedReminaderPlusOne(hash, replica.shard_count);
	System.out.println("REPLICA SHARD ID " + replica.shard_id + " REPLICA SHARD COUNT " + replica.shard_count + " HASH: " + hash +" TARGET SHARD: " + target_shard);
	if (replica.shard_id != target_shard) {
		resp = replica.forwardShardDELETE(target_shard, key, jsonData, sender_url);
	} else {
		resp = replica.deleteKey(key,  String.valueOf(jsonData.get("causal-metadata")), jsonData, sender_url);
	}

	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}


@RequestMapping (value = "/retrieve-key-value-store", method  = RequestMethod.GET, produces = "application/json")
public ResponseEntity<String> retrieveKVStore(HttpServletRequest request) {
	Response resp = null;
	URL sender_url = appURL.processURL(request.getRemoteAddr() + ":" + 8080);
	// if (!replica.replicaExistsInView(sender_url)){
	// 	resp = Response.createJsonResponse(400, Response.pair("error", "not authorized, replicas only"));
	// 	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	// }
	// if (replica.kvStore.kvStoreisEmpty()){
	// 	resp = Response.createJsonResponse(401, Response.pair("error", "kvStore is empty"));
	// 	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	// }
	String kvJson = replica.retrieveKVStore_Reponse();
	System.out.println("REQUESTED KVSTORE JSON: \n" + kvJson);
	Response kvResp = Response.createJsonResponse(200, Response.pair("garbage", "this is stupid"));
	return new ResponseEntity<String>(kvJson, kvResp.httpCode);
}

@RequestMapping(value = "/key-value-store-view-debug", method = RequestMethod.GET, produces = "text/html")
public ResponseEntity<String> debugView() {
	return new ResponseEntity<String>(replica.toString(), HttpStatus.OK);
}

@RequestMapping(value = "/key-value-store-view", method = RequestMethod.GET, produces = "application/json")
public ResponseEntity<String> getView() {
	String viewStr = replica.getViewString();
	Response resp = Response.createJsonResponse(200, Response.pair("message", "View retrieved successfully"), Response.pair("view", viewStr));
	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}


@RequestMapping(value = "/key-value-store-view", method = RequestMethod.PUT, produces = "application/json")
public ResponseEntity<String> putView(@RequestBody Map<String, Object> jsonData, HttpServletRequest request) {

	System.out.println(jsonData.toString());

	if (!jsonData.containsKey("socket-address")) {

		Response socketMissing = Response.createJsonResponse(
			400,
			Response.pair("error", "Value is missing"),
			Response.pair("message", "Error in PUT")
			);
		return new ResponseEntity<String>(socketMissing.responseString, socketMissing.httpCode);

	} else {

		String replicaAddress = String.valueOf(jsonData.get("socket-address")).trim();
		URL replicaURL = appURL.processURL(replicaAddress);
		System.out.println(replicaURL);
		Response alreadyExists = Response.createJsonResponse(404, Response.pair("error", "Socket address already exists in the view"), Response.pair("message", "Error in PUT"));
		ResponseEntity<String> alreadyExistsJsonResp = new ResponseEntity<String>(alreadyExists.responseString, alreadyExists.httpCode);

		URL senderURL = appURL.processURL(request.getRemoteAddr() + ":" + 8080);

		if (replica.replicaExistsInView(replicaURL)) {

			System.out.printf("PUT: %s ALREADY EXISTED\n", replicaURL.toString());
			return alreadyExistsJsonResp;

		} else {
			replicaAddress = String.valueOf(jsonData.get("socket-address")).trim();
			replicaURL = appURL.processURL(replicaAddress);
			System.out.println(replicaURL);
			alreadyExists = Response.createJsonResponse(404, Response.pair("error", "Socket address already exists in the view"), Response.pair("message", "Error in PUT"));
			alreadyExistsJsonResp = new ResponseEntity<String>(alreadyExists.responseString, alreadyExists.httpCode);
			if (replica.replicaExistsInView(replicaURL)) {
				System.out.printf("PUT: %s ALREADY EXISTED\n", replicaURL.toString());
				return alreadyExistsJsonResp;
			} else {
				if (replica.addReplica(replicaURL)) {
					System.out.printf("SENDING BROADCAST FROM %s to %s\n", replica.getSocketAddress().toString(), replica.getViewString());
					Broadcast.send(replica, "PUT", "/key-value-store-view", new Gson().toJson(jsonData).toString());
					Response successfullyAdded = Response.createJsonResponse(200, Response.pair("message", "Replica added successfully to the view"));
					return new ResponseEntity<String>(successfullyAdded.responseString, successfullyAdded.httpCode);
				} else {
					System.out.printf("PUT: %s ALREADY EXISTED.\n", replicaURL.toString());
					return alreadyExistsJsonResp;
				}
			}
		}
	}
}

@RequestMapping(value = "/key-value-store-view", method = RequestMethod.DELETE, produces = "application/json")
public ResponseEntity<String> deleteView(@RequestBody Map<String, Object> jsonData, HttpServletRequest request) {
	System.out.println("key-value-store-view DELETE ");
	System.out.println(jsonData.toString());
	if (!jsonData.containsKey("socket-address")) {

		Response socketMissing = Response.createJsonResponse(
			400,
			Response.pair("error", "Value is missing"),
			Response.pair("message", "Error in DELETE")
			);
		return new ResponseEntity<String>(socketMissing.responseString, socketMissing.httpCode);
	} else {
		String replicaURLStr = appURL.processURL(jsonData.get("socket-address").toString()).toString();
		// default response if replica is not in view
		Response notExists = Response.createJsonResponse(404, Response.pair("error", "Socket address does not exist in the view"), Response.pair("message", "Error in DELETE"));
		ResponseEntity<String> notExistsJsonResp = new ResponseEntity<String>(notExists.responseString, notExists.httpCode);

		// gets the requesters address so we don't resend delete message to it

		String requestURLStr = String.format("%s:%d", request.getRemoteAddr(), 8080);
		URL requestURL = appURL.processURL(requestURLStr);
		URL replicaURL = appURL.processURL(replicaURLStr);
		System.out.println("DELETE REQUESTED FROM: " + requestURL);
		System.out.println("DELETE REQUESTED FOR: " + replicaURL);

		//if not in view
		if (!replica.replicaExistsInView(replicaURL)) {
			System.out.println("DOESN'T EXIST!");
			return notExistsJsonResp;
		} else {
			System.out.println("EXISTS!");
			replica.broadcastDeleteView(replicaURL, requestURL);
			Response resp = Response.createJsonResponse(200, Response.pair("message", "Replica deleted successfully from the view"));
			ResponseEntity<String> successResp = new ResponseEntity<String>(resp.responseString, resp.httpCode);
			return successResp;
		}
	}
}


//shard operations
// get shard ids of the store
// pass
@RequestMapping(value = "/key-value-store-shard/shard-ids", method = RequestMethod.GET, produces = "application/json")
public ResponseEntity<String> getShardIDs(){
	String shard_ids_Str = replica.getIDsString();
	Response resp = Response.createJsonResponse(200, Response.pair("message", "Shard IDs retrieved successfully"), Response.pair("shard-ids", shard_ids_Str));
	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}

//get the shard ID of a node
// problem: all nodes are in shard 1 and shard 2 has no node.
@RequestMapping(value = "/key-value-store-shard/node-shard-id", method = RequestMethod.GET, produces = "application/json")
public ResponseEntity<String> getShardIDofNode(){
	String shard_id_of_node_Str = replica.shard_id.toString();
	Response resp = Response.createJsonResponse(200, Response.pair("message", "Shard ID of the node retrieved successfully"), Response.pair("shard-id", shard_id_of_node_Str));
	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}

// get the members of a shard id
// problem: since all the nodes are initially partitioned to the first shard,
//          if we send GET request to the second shard, it will return String index out of range
//          Other than that, according to the pdf, we should return a list of members in the list
@RequestMapping(value = "/key-value-store-shard/shard-id-members/{id}", method = RequestMethod.GET, produces = "application/json")
public ResponseEntity<String> getMembersOfShardID(@PathVariable String id){
	// This should actually be a list of members
	String shard_id_members_Str = replica.getMembersOfID(id);
	Response resp = Response.createJsonResponse(200, Response.pair("message", "Members of shard ID retrieved successfully"), Response.pair("shard-id-members", shard_id_members_Str));
	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}

// get the number of keys stored in a shard
// Problem: we should first solve the sharding problem above so that we can determine which nodes
//          are in which shards.
//          It is now return 0 for count
@RequestMapping(value = "/key-value-store-shard/shard-id-key-count/{shardId}", method = RequestMethod.GET, produces = "application/json")
public ResponseEntity<String> getShardKeyCount(@PathVariable String shardId){
	if (replica.shard_id.equals(Integer.parseInt(shardId))) {
		int shardIdCount = replica.kvStore.values.size();
		Response resp = Response.createJsonResponse(200, Response.pair("message", "Key count of shard ID retrieved successfully"), Response.pair("shard-id-key-count", shardIdCount));
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	} else {
		Response resp = replica.forwardShardKeyCount(Integer.parseInt(shardId));
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	}
}

// add a node to a shard // to-do
// Note from pdf: To add a new node to the store, we start the corresponding Docker container without the SHARD_COUNT environment variable.
// Afterwards, we send a PUT request to explicitly add the new node to a particular shard.
@RequestMapping(value = "/key-value-store-shard/add-member/{shardId}", method = RequestMethod.PUT, produces = "application/json")
public ResponseEntity<String> addNodeToShard(@PathVariable String shardId, @RequestBody Map<String, Object> jsonData, HttpServletRequest request){

	String new_socket_address = jsonData.get("socket-address").toString();
	URL new_socket_address_URL = appURL.processURL(new_socket_address);

	if (new_socket_address_URL.equals(replica.getSocketAddress())) {
		replica.shard_id = Integer.parseInt(shardId);
		replica.retrieveKVStore_Request_Shard();
	}

	boolean added = replica.addToShard(shardId, new_socket_address);

	if(added){
		Response resp = Response.createJsonResponse(200, Response.pair("message", "Node added successfully"), Response.pair("socket-address", new_socket_address));
		URL sender_url = appURL.processURL(request.getRemoteAddr() + ":" + 8080);
		Broadcast.send(replica, "PUT", "/key-valuestore-shard/add-member" + shardId, new Gson().toJson(jsonData), sender_url);
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	}else{
		Response resp = Response.createJsonResponse(405, Response.pair("message", "Node added failed"), Response.pair("socket-address",new_socket_address));
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	}

}

// resharding the key-value store
// Note from pdf: Resharding is initiated whenever the administrator (from a client) sends a resharding request to a node.
// That is, nodes do not automatically initiate resharding on their own when the number of nodes in a shard
// decreases to 1 or new nodes are added to the store.
@RequestMapping(value = "/key-value-store-shard/reshard", method = RequestMethod.PUT, produces = "application/json")
public ResponseEntity<String> reshard(@RequestBody Map<String, Object> jsonData){
	if(replica.reshard(jsonData.get("shard-count").toString())){
		Response resp = Response.createJsonResponse(200, Response.pair("message", "Resharding done successfully"));
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	}else{
		Response resp = Response.createJsonResponse(400, Response.pair("message", "Resharding fails"));
		return new ResponseEntity<String>(resp.responseString, resp.httpCode);
	}

}

//get shard map from this replica
@RequestMapping(value = "/key-value-store-shard/get-shard-map", method = RequestMethod.GET)
public ResponseEntity<String> getShardMap(){
	return new ResponseEntity<String>(new Gson().toJson(replica.shard_map).toString(), HttpStatus.OK);
}

//set shard_map to new shard_map after resharding or adding new nodes
@RequestMapping(value = "/key-value-store-shard/set-shard-map", method = RequestMethod.PUT, produces = "application/json")
public ResponseEntity<String> setShardMap(@RequestBody Map<String, Object> jsonData, HttpServletRequest request){
	Response resp = null;
	resp = replica.setShardMap(jsonData);
	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}

@RequestMapping(value = "/key-value-store-shard/set-shard-id", method = RequestMethod.PUT, produces = "application/json")
public ResponseEntity<String> setShardID(@RequestBody Map<String, Object> jsonData, HttpServletRequest request){
	Response resp = null;
	resp = replica.setShardID(jsonData.get("ID").toString());
	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}

//get kvstore from this replica
@RequestMapping(value = "/key-value-store-shard/get-key-value-store", method = RequestMethod.GET)
public ResponseEntity<String> getkvStore(){
	return new ResponseEntity<String>(new Gson().toJson(replica.kvStore).toString(), HttpStatus.OK);
}

@RequestMapping(value = "/notify-metadata", method = RequestMethod.PUT, produces = "application/json")
public ResponseEntity<String> notifyMetadata(@RequestBody Map<String, Object> jsonData) {
	System.out.println("NOTIFY METADATA RECEIVED \n" + jsonData.toString());
	Response resp = replica.updateMetadata(String.valueOf(jsonData.get("withVersion")));
	return new ResponseEntity<String>(resp.responseString, resp.httpCode);
}

/*******************************************************************************************/



public static AtomicInteger heartbeat_enable = new AtomicInteger();

@Scheduled(fixedDelay = 5000, initialDelay = 30000)
public void heartbeat() {
	System.out.println("<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3");
	System.out.println("heartbeat_enable " + heartbeat_enable.intValue());
	if (heartbeat_enable.get() == 0) {
		System.out.println("ping");
		HashMap<URL, Integer> map = Broadcast.send(replica, "GET", "/ping", "");
		for (Map.Entry<URL, Integer> entry : map.entrySet() ) {
			URL target_replica_url = entry.getKey();
			int response_code = entry.getValue();

			if (response_code == 503) {
				System.out.println("Replica at " + target_replica_url.toString() + " is down, deleting");
				replica.broadcastDeleteView(target_replica_url, replica.getSocketAddress());
			}
		}
	}
	System.out.println("<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3<3");
}

public static void addedToNetwork() {
	JsonObject jsonData = new JsonObject();
	jsonData.addProperty("socket-address", replica.getSocketAddress().toString().replaceFirst("http://", ""));
	Broadcast.send(replica, "PUT", "/key-value-store-view", jsonData.toString());
	//replica.retrieveKVStore_Request_Shard();
}

@GetMapping(value="/ping", produces = "application/json")
public ResponseEntity<String> ping() {
	return new ResponseEntity<String>("PONG", HttpStatus.OK);
}

@GetMapping(value="/debug", produces = "text/html")
public ResponseEntity<String> debugKV() {
	return new ResponseEntity<String>(this.replica.kvStore.toString(), HttpStatus.OK);
}

public static void main(String[] args) {


	replica = new Replica(System.getenv());
	SpringApplication.run(Application.class, args);

	try {
		Thread.sleep(5000);
	} catch (Exception e) {}

	addedToNetwork();

}

}
