package app;

import java.net.URL;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.io.IOException;

import java.util.UUID;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;

public class Replica {

public HashSet<URL> view;
public KeyValueStore kvStore;
private URL socketAddress;
public Integer shard_id;
public Integer shard_count;
public HashMap<Integer, ArrayList<URL>> shard_map;//a whole picture of shard system eg. {1:[node1,node3,node4];2:[node2,node5,node6]}


public Replica(Map<String, String> environmentVariables) {
	this.kvStore = new KeyValueStore();

	// grab initiliazation data from environment
	String socketAddressStr = environmentVariables.get("SOCKET_ADDRESS");
	String viewStr = environmentVariables.get("VIEW");


	// if socket address exists
	if (null == socketAddressStr) {
		throw new RuntimeException("You didn't specify a socket address, specify like \"-e SOCKET_ADDRESS=10.10.0.2:8080");
	}
	// set socket address
	this.socketAddress = appURL.processURL(socketAddressStr);
	this.view = new HashSet<URL>();
	if (null == viewStr || viewStr.trim().length() == 0) {
		// do nothing
		// if there are commas; multiple instances
	} else if (viewStr.contains(",")) {
		String[] viewURLs = viewStr.split(",");
		for (String viewURLStr : viewURLs) {
			view.add(appURL.processURL(viewURLStr));
		}
		// if theres only one other replica
	} else {
		view.add(appURL.processURL(viewStr));
	}

	// get SHARD_COUNT from environment variable
	String shard_count_str = environmentVariables.get("SHARD_COUNT");
	if (shard_count_str != null) {
		this.shard_count = Integer.parseInt(shard_count_str);
	}


	//get shard_map from other replica
	//if gets empty map, it means the first replica, then assign all replicas in its view to different shards
	this.shard_map = new HashMap<Integer, ArrayList<URL>>();
	// JsonObject jsonData = new JsonObject();
	// jsonData.addProperty("socket-address", this.socketAddress.toString().replaceFirst("http://", ""));
	// Broadcast.send(this, "PUT", "/key-value-store-view", jsonData.toString());

	retrieveKVStore_Request();
	//this.shard_map = getShardMapFrom(this.view);


	if (this.shard_map.isEmpty()) {
		this.shard_map = assignNodesToShard(this.view, this.shard_count);
	}

	if (this.shard_count == null) {
		this.shard_count = this.shard_map.size();
	}

	//find its own shard_id in shard_map
	this.shard_id = 0;
	if (!this.shard_map.isEmpty()) {
		for (Integer id : this.shard_map.keySet()) {
			if (this.shard_map.get(id).contains(this.socketAddress)) {
				this.shard_id = id;
				break;
			}
		}

	}
}

@Override
public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append("================================");
	sb.append(String.format("Socket URL: %s", socketAddress.toString()));
	sb.append("\nView:\n");
	for (URL otherReplicas: view) {
		sb.append(otherReplicas.toString());
		sb.append("\n");
	}
	sb.append("================================");
	return sb.toString();
}


public URL getSocketAddress() {
	return new appURL(this.socketAddress).getURL();
}

public Response updateMetadata(String version) {
	return this.kvStore.updateMetadata(version);
}

public Response forwardShardPUT(int target_shard, String key, Map<String, Object> jsonData, URL sender_url) {
	Application.heartbeat_enable.getAndIncrement();

	Object value = jsonData.get("value");
	System.out.println("FORWARDING REQUEST FROM " + this.socketAddress.toString() + "\n KEY: "+ key + " VALUE: " + value + " TO SHARD: "+ target_shard);
	jsonData.put("firstForward", true);
	String jsonString = new Gson().toJson(jsonData);
	Response returnResp = Response.createJsonResponse(500, Response.pair("error", "shard forward failure"));

	try {
		for (URL target_replica_url : this.shard_map.get(target_shard)) {
			if (!target_replica_url.equals(this.getSocketAddress())) {
				RestTemplate restTemplate = new RestTemplate();
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

				URL endpoint = appURL.addPath(target_replica_url, "/key-value-store/" + key);
				System.out.println("ENDPOINT: " + endpoint.toString());

				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);

				HttpEntity<String> request = new HttpEntity<String>(jsonString, headers);

				System.out.println("Broadcasting PUT to " + target_replica_url.toString());
				ResponseEntity<String> resp;
				try {
					resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("PUT"), request, String.class);
					returnResp = new Response( resp.getBody(), resp.getStatusCodeValue());
					Application.heartbeat_enable.getAndDecrement();
					return returnResp;
				} catch (RestClientException e) {
					System.out.println("Broadcast to " + target_replica_url.toString() +
														 " resulted in RestClientException");
					continue;
				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION: " + e.toString());
	}
	Application.heartbeat_enable.getAndDecrement();
	return returnResp;
}

public Response forwardShardGET(int target_shard, String key) {
	Application.heartbeat_enable.getAndIncrement();
	ResponseEntity<String> resp = null;

	System.out.println("FORWARDING GET FOR KEY '" + key + "'FROM " + this.socketAddress.toString() + "TO TARGET SHARD " + String.valueOf(target_shard));

	try {
		for (URL target_replica_url : this.shard_map.get(target_shard)) {
			if (!target_replica_url.equals(this.getSocketAddress())) {

				RestTemplate restTemplate = new RestTemplate();
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

				URL endpoint = appURL.addPath(target_replica_url, "/key-value-store/" + key);
				System.out.println("ENDPOINT: " + endpoint.toString());

				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);

				HttpEntity<String> request = new HttpEntity<String>(headers);

				System.out.println("SENDING GET for KEY '" + key + "' to " + target_replica_url.toString());

				try {
					resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("GET"), request, String.class);
					Response returnResp = new Response( resp.getBody(), resp.getStatusCodeValue());
					Application.heartbeat_enable.getAndDecrement();
					return returnResp;

				} catch (RestClientException e) {
					System.out.println("Broadcast to " + target_replica_url.toString() +
														 " resulted in RestClientException");
					continue;
				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION: " + e.toString());
	}

	// if resp returned without a RestClientException, then this would have already happened
	// before returnResp was returned
	// else, each try/catch in the for loop above failed every time so we decrement here.
	Application.heartbeat_enable.getAndDecrement();
	return Response.createJsonResponse(500, Response.pair("error", "shard foward get failure"));
}

public Response forwardShardDELETE(int target_shard, String key, Map<String, Object> jsonData, URL sender_url) {
	Application.heartbeat_enable.getAndIncrement();

	Object value = jsonData.get("value");
	System.out.println("FORWARDING REQUEST FROM " + this.socketAddress.toString() + "\n KEY: "+ key + " VALUE: " + value + " TO SHARD: "+ target_shard);
	jsonData.put("firstForward", true);
	String jsonString = new Gson().toJson(jsonData);
	Response returnResp = Response.createJsonResponse(500, Response.pair("error", "shard forward failure"));

	try {
		for (URL target_replica_url : this.shard_map.get(target_shard)) {
			if (!target_replica_url.equals(this.getSocketAddress())) {
				RestTemplate restTemplate = new RestTemplate();
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

				URL endpoint = appURL.addPath(target_replica_url, "/key-value-store/" + key);
				System.out.println("ENDPOINT: " + endpoint.toString());

				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);

				HttpEntity<String> request = new HttpEntity<String>(jsonString, headers);

				System.out.println("Broadcasting DELETE to " + target_replica_url.toString());
				ResponseEntity<String> resp;
				try {
					resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("DELETE"), request, String.class);
					returnResp = new Response( resp.getBody(), resp.getStatusCodeValue());
					Application.heartbeat_enable.getAndDecrement();
					return returnResp;
				} catch (RestClientException e) {
					System.out.println("Broadcast to " + target_replica_url.toString() +
														 " resulted in RestClientException");
					continue;
				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION: " + e.toString());
	}
	Application.heartbeat_enable.getAndDecrement();
	return returnResp;
}

public Response forwardShardKeyCount(int target_shard) {
	Application.heartbeat_enable.getAndIncrement();
	ResponseEntity<String> resp = null;

	try {
		for (URL target_replica_url : this.shard_map.get(target_shard)) {
			if (!target_replica_url.equals(this.getSocketAddress())) {

				RestTemplate restTemplate = new RestTemplate();
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

				URL endpoint = appURL.addPath(target_replica_url, "/key-value-store-shard/shard-id-key-count/" + target_shard);
				System.out.println("ENDPOINT: " + endpoint.toString());

				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);

				HttpEntity<String> request = new HttpEntity<String>(headers);

				try {
					resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("GET"), request, String.class);
					Response returnResp = new Response( resp.getBody(), resp.getStatusCodeValue());
					Application.heartbeat_enable.getAndDecrement();
					return returnResp;

				} catch (RestClientException e) {
					System.out.println("Broadcast to " + target_replica_url.toString() +
														 " resulted in RestClientException");
					continue;
				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION: " + e.toString());
	}

	// if resp returned without a RestClientException, then this would have already happened
	// before returnResp was returned
	// else, each try/catch in the for loop above failed every time so we decrement here.
	Application.heartbeat_enable.getAndDecrement();
	return Response.createJsonResponse(500, Response.pair("error", "shard foward get failure"));
}

public Response putKey(String key, Object value, String dependsOn, Map<String, Object> jsonData, URL sender_url) {
	System.out.println("RECEIVED PUT REQUEST FROM " + sender_url.toString() + "\n KEY: "+ key + " VALUE: " + value + " VERSION " + dependsOn);

	// forwarded from another kv-store
	if (jsonData.containsKey("withVersion")) {
		this.kvStore.put(key, value, jsonData.get("withVersion").toString(), this.shard_id);
		return Response.createJsonResponse(200,
				Response.pair("message", "Replicated Successfully"),
				Response.pair("version", this.kvStore.versionForKey(key)));
	}

	Response resp;
	int timeout = 10;
	String mostRecentDependsOn = "";
	dependsOn = dependsOn.trim();

	if (dependsOn == null || dependsOn.isEmpty()) {
		// doesnt depend on anything
	} else {

		// get most recent version
		int commaIndex = dependsOn.indexOf(',');
		if (commaIndex == -1) {
			mostRecentDependsOn = dependsOn;
		} else {
			mostRecentDependsOn = dependsOn.split(",")[0];
		}

		if (this.kvStore.versionExisted(mostRecentDependsOn)) {
			// depends on version is in history
		} else {
			System.err.printf("PUT: WAITING FOR DEPENDENT KEY: %s", mostRecentDependsOn);
			while (timeout >= 0) {
				timeout--;
				try {
					if (this.kvStore.versionExisted(mostRecentDependsOn)) {
						break;
					}
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					System.err.println("PUT: INTERRUPTED WHILE WAITING FOR " + mostRecentDependsOn);
				}
			}
		}

		if (this.kvStore.versionExisted(mostRecentDependsOn)) {
			// depends on is in history
		} else {
			return Response.createJsonResponse(308,
					Response.pair("message", "error in PUT"),
					Response.pair("error", String.format("timeout while waiting for key '%s'", mostRecentDependsOn))
				);
		}

	}

	// fetch response from kvstore
	resp = this.kvStore.put(key, value, null, this.shard_id);
	if (resp.httpCode == HttpStatus.OK || resp.httpCode == HttpStatus.CREATED) {
		jsonData.put("withVersion", this.kvStore.versionForKey(key));
		if (jsonData.containsKey("firstForward") && jsonData.get("firstForward").equals(true)) {
			sender_url = appURL.processURL("fakeurl");
			jsonData.put("firstForward", false);
		}
		Broadcast.sendToShards(this, "PUT", new String("/key-value-store/" + key), new Gson().toJson(jsonData).toString(), sender_url);
	}
	return resp;
}

public Response deleteKey(String key, String dependsOn, Map<String, Object> jsonData, URL sender_url) {
	System.out.println("RECEIVED DELETE REQUEST FROM " + sender_url.toString() + "\n KEY: "+ key + " VERSION " + dependsOn);

	// forwarded from another kv-store
	if (jsonData.containsKey("withVersion")) {
		this.kvStore.delete(key, jsonData.get("withVersion").toString());
		return Response.createJsonResponse(200,
				Response.pair("message", "Replicated Successfully"),
				Response.pair("version", this.kvStore.versionForKey(key))
				);
	}

	Response resp;

	int timeout = 10;
	String mostRecentDependsOn = "";
	dependsOn = dependsOn.trim();

	if (dependsOn == null || dependsOn.isEmpty()) {
		// doesnt depend on anything
	} else {

		// get most recent version
		int commaIndex = dependsOn.indexOf(',');
		if (commaIndex == -1) {
			mostRecentDependsOn = dependsOn;
		} else {
			mostRecentDependsOn = dependsOn.split(",")[0];
		}

		if (this.kvStore.versionExisted(mostRecentDependsOn)) {
			// depends on version is in history
		} else {
			System.err.printf("DELETE: WAITING FOR DEPENDENT KEY: %s", mostRecentDependsOn);
			while (timeout >= 0) {
				timeout--;
				try {
					if (this.kvStore.versionExisted(mostRecentDependsOn)) {
						break;
					}
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					System.err.println("DELETE: INTERRUPTED WHILE WAITING FOR " + mostRecentDependsOn);
				}
			}
		}

		if (this.kvStore.versionExisted(mostRecentDependsOn)) {
			// depends on is in history
		} else {
			return Response.createJsonResponse(308,
					Response.pair("message", "error in PUT"),
					Response.pair("error", String.format("timeout while waiting for key '%s'", mostRecentDependsOn))
				);
		}

	}

	// fetch response from kvstore
	resp = this.kvStore.delete(key, null);
	if (resp.httpCode == HttpStatus.OK) {
		jsonData.put("withVersion", this.kvStore.versionForKey(key));
		Broadcast.send(this, "DELETE", new String("/key-value-store/" + key), new Gson().toJson(jsonData).toString(), sender_url);
	}
	return resp;
}

public String retrieveKVStore_Reponse() {
	JsonObject kvObj = this.kvStore.toJsonObject();
	JsonObject shardObj = new JsonObject();
	for (Map.Entry<Integer, ArrayList<URL>> entry : shard_map.entrySet()){
		String shardURLStr = new Gson().toJson(entry.getValue());
		JsonArray shardURL = new Gson().fromJson(shardURLStr, JsonArray.class);
		shardObj.add(entry.getKey().toString(), shardURL);
	}
	kvObj.add("shard_map", shardObj);
	return kvObj.toString();
}

public void retrieveKVStore_Request() {

	for (URL target_replica_url : this.view) {

		if (!target_replica_url.equals(this.getSocketAddress())) {
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			RestTemplate restTemplate = new RestTemplate();
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

			URL endpoint = appURL.addPath(target_replica_url, "/retrieve-key-value-store");

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);

			HttpEntity<String> request = new HttpEntity<String>("", headers);

			ResponseEntity<String> resp;

			try {

				resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("GET"), request, String.class);

			} catch (RestClientException e) {

				System.out.println("RETRIEVE KVSTORE to " + target_replica_url.toString() +
													 " resulted in RestClientException");
				continue;
			}

			System.out.println("JSON RESPONSE: " + resp.getBody());

			JsonElement jelem = new Gson().fromJson(resp.getBody(), JsonElement.class);
			JsonObject jobj = jelem.getAsJsonObject();

			System.out.println("JSON OBJECT: " + jobj.toString());

			JsonObject valuesJson = jobj.getAsJsonObject("values");
			Type stringObjMap = new TypeToken<Map<String, Object>>(){}.getType();
			Map<String,Object> valuesMap = new Gson().fromJson(valuesJson.toString(), stringObjMap);

			JsonObject versionJson = jobj.getAsJsonObject("version");
			Type stringStringMap = new TypeToken<Map<String, String>>(){}.getType();
			Map<String,String> versionMap = new Gson().fromJson(versionJson.toString(), stringObjMap);

			JsonArray orderedJson = jobj.getAsJsonArray("orderedVersion");
			LinkedList<String> orderedList = new LinkedList<String>();
			for (int i =0; i < orderedJson.size(); i++) {
				orderedList.push(orderedJson.get(i).getAsString());
			}

			JsonObject shard_mapJson = jobj.getAsJsonObject("shard_map");
			HashMap<Integer, ArrayList<URL>> shard_mapObj = new HashMap<Integer, ArrayList<URL>>();
			for (int i = 1; i <= shard_mapJson.size(); i++) {
				ArrayList<URL> list = new ArrayList<URL>();
				JsonArray array = shard_mapJson.getAsJsonArray(Integer.toString(i));
				for (int j = 0; j < array.size(); j++) {
					try {list.add(new URL(array.get(j).getAsString()));}
					catch (MalformedURLException e) {System.out.println("CAUGHT MalformedURLException");}
				}
				shard_mapObj.put(i, list);
			}

			System.out.println("RESPONSE values MAP: \n" + valuesMap.toString());
			System.out.println("RESPONSE version MAP: \n" + versionMap.toString());
			System.out.println("RESPONSE orderedVersion LIst: \n" + orderedList.toString());
			System.out.println("RESPONSE shard_map: \n" + shard_mapObj.toString());

			this.kvStore.values = new HashMap<String, Object>(valuesMap);
			this.kvStore.version = new HashMap<String, String>(versionMap);
			this.kvStore.orderedVersion = new LinkedList<String>(orderedList);
			this.shard_map = new HashMap<Integer, ArrayList<URL>>(shard_mapObj);
			this.shard_count = this.shard_map.size();
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			return;
		}
	}
}

public void retrieveKVStore_Request_Shard() {

	for (URL target_replica_url : this.shard_map.get(this.shard_id)) {

		if (!target_replica_url.equals(this.getSocketAddress())) {
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			RestTemplate restTemplate = new RestTemplate();
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

			URL endpoint = appURL.addPath(target_replica_url, "/retrieve-key-value-store");

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);

			HttpEntity<String> request = new HttpEntity<String>("", headers);

			ResponseEntity<String> resp;

			try {

				resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("GET"), request, String.class);

			} catch (RestClientException e) {

				System.out.println("RETRIEVE KVSTORE to " + target_replica_url.toString() +
													 " resulted in RestClientException");
				continue;
			}

			System.out.println("JSON RESPONSE: " + resp.getBody());

			JsonElement jelem = new Gson().fromJson(resp.getBody(), JsonElement.class);
			JsonObject jobj = jelem.getAsJsonObject();

			System.out.println("JSON OBJECT: " + jobj.toString());

			JsonObject valuesJson = jobj.getAsJsonObject("values");
			Type stringObjMap = new TypeToken<Map<String, Object>>(){}.getType();
			Map<String,Object> valuesMap = new Gson().fromJson(valuesJson.toString(), stringObjMap);

			JsonObject versionJson = jobj.getAsJsonObject("version");
			Type stringStringMap = new TypeToken<Map<String, String>>(){}.getType();
			Map<String,String> versionMap = new Gson().fromJson(versionJson.toString(), stringObjMap);

			JsonArray orderedJson = jobj.getAsJsonArray("orderedVersion");
			LinkedList<String> orderedList = new LinkedList<String>();
			for (int i =0; i < orderedJson.size(); i++) {
				orderedList.push(orderedJson.get(i).getAsString());
			}

			JsonObject shard_mapJson = jobj.getAsJsonObject("shard_map");
			HashMap<Integer, ArrayList<URL>> shard_mapObj = new HashMap<Integer, ArrayList<URL>>();
			for (int i = 1; i <= this.shard_count; i++) {
				ArrayList<URL> list = new ArrayList<URL>();
				JsonArray array = shard_mapJson.getAsJsonArray(Integer.toString(i));
				for (int j = 0; j < array.size(); j++) {
					try {list.add(new URL(array.get(j).getAsString()));}
					catch (MalformedURLException e) {System.out.println("CAUGHT MalformedURLException");}
				}
				shard_mapObj.put(i, list);
			}

			System.out.println("RESPONSE values MAP: \n" + valuesMap.toString());
			System.out.println("RESPONSE version MAP: \n" + versionMap.toString());
			System.out.println("RESPONSE orderedVersion LIst: \n" + orderedList.toString());
			System.out.println("RESPONSE shard_map: \n" + shard_mapObj.toString());

			this.kvStore.values = new HashMap<String, Object>(valuesMap);
			this.kvStore.version = new HashMap<String, String>(versionMap);
			this.kvStore.orderedVersion = new LinkedList<String>(orderedList);
			this.shard_map = new HashMap<Integer, ArrayList<URL>>(shard_mapObj);
			this.shard_count = this.shard_map.size();
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			return;
		}
	}
}

// returns string like 'url1,url2,url3'
public String getViewString() {
	List<String> list = new ArrayList<String>();

	for (URL remoteReplica: view) {
		String urlstr = remoteReplica.toString();
		String rawSocketAddress = urlstr.replaceFirst("http://", "");
		list.add(rawSocketAddress);
	}

	Collections.sort(list);
	String viewString = list.stream().collect(Collectors.joining(","));
	return viewString;
}

// true if the replica exists in the view, false if replica has already been added
public boolean replicaExistsInView(URL newReplica) {
	if (socketAddress.equals(newReplica)) {
		debug.msg("replica " + newReplica + "exists in the view");
		return true;
	}
	return view.contains(newReplica);
}

// true if successfully added, false if replica is already in view
public boolean addReplica(URL newReplica) {
	if (replicaExistsInView(newReplica)) {
		debug.msg("replica "+ newReplica + " exists in the view");
		return false;
	}
	view.add(newReplica);
	debug.msg("add replica "+ newReplica + " to the view");
	return true;
}

public boolean deleteReplica(String toDelete) {
	// doesnt check if the replica to delete is this own replica, that should be done
	// in Application.java and corresponding PUT broadcast sent there
	URL deleteURL = appURL.processURL(toDelete);
	if (!view.contains(deleteURL)) {
		return false;
	}
	view.remove(deleteURL);
	debug.msg("delete replica " + deleteURL + " from the view");
	return true;
}

public static HttpURLConnection prepareConnection(String requestType, URL url) throws IOException {
	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	conn.setDoOutput(true);
	conn.setRequestMethod(requestType);
	conn.setRequestProperty("Content-Type", "application/json");
	conn.setRequestProperty("Accept", "application/json");
	// allow differentiation between requests from server
	// to server and from user to the main instance
	//conn.setRequestProperty("User-Agent", Response.serverUserAgent);
	return conn;

}

//send msg to check whether target_replica_url is on
public Response sendCheckMsg(URL target_replica_url, String requestType){


	requestType = requestType.toUpperCase();

	URL endpoint = appURL.addPath(target_replica_url, "/key-value-store-view");

	InputStream is;
	int responseCode = 0;

	HttpURLConnection conn;
	try {
		conn = Replica.prepareConnection(requestType, endpoint);
		is = conn.getInputStream();
		responseCode = conn.getResponseCode();
	} catch (IOException e) {
		e.printStackTrace();
	}

	if (responseCode == 200) {
		return new Response("Successful ping!", responseCode);
	} else {
		return new Response(String.format("Response failed with error code %d", responseCode), 200);
	}

}

public void broadcastDeleteView(URL target_delete_url, URL sender_url) {
	Application.heartbeat_enable.getAndIncrement();
	try {
		this.view.remove(target_delete_url);
		for(URL target_replica_url : this.view) {
			if (!target_replica_url.equals(this.socketAddress) && !target_replica_url.equals(sender_url)) {
				System.out.println(target_replica_url.toString());
				RestTemplate restTemplate = new RestTemplate();
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

				HttpHeaders headers = new HttpHeaders();

				URL endpoint = appURL.addPath(target_replica_url, "/key-value-store-view");
				System.out.println("ENDPOINT: " + endpoint.toString());

				HashMap<String, String> requestMap = new HashMap<String, String>();
				requestMap.put("socket-address", target_delete_url.toString().substring(7));   //cut out the leading http://
				String requestJson = new Gson().toJson(requestMap);

				headers.setContentType(MediaType.APPLICATION_JSON);

				HttpEntity<String> request = new HttpEntity<String>(requestJson, headers);

				try {
					restTemplate.exchange(endpoint.toString(), HttpMethod.DELETE, request, Void.class);
				} catch (RestClientException e) {
					System.out.println("broadcastDeleteView on address " + target_replica_url.toString() +
														 " resulted in RestAccessException ");
					continue;
				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION IN BROADCAST DELETE VIEW: " + e.toString());
	}
	Application.heartbeat_enable.getAndDecrement();
}

public String getIDsString(){
	StringBuffer shard_ids_StrBuff = new StringBuffer();
	for(int i=1;i<=shard_count;i++){
		shard_ids_StrBuff.append(String.valueOf(i));
		if(i<shard_count){
			shard_ids_StrBuff.append(",");
		}
	}
	// System.out.println(shard_ids_StrBuff);

	String shard_ids_Str = shard_ids_StrBuff.toString();
	return shard_ids_Str;
}

// still having problem if it tries to add a second node
public HashMap<Integer,ArrayList<URL>> assignNodesToShard(HashSet<URL> target, Integer count){
		HashMap<Integer,ArrayList<URL>> shard_map = new HashMap<>();
		try{
			if(target.size()>=2*count){
				Integer ind=1;
				for(URL rep_url : target){
					if(!shard_map.keySet().contains(ind)){
						shard_map.put(ind ,new ArrayList<URL>());
					}
					shard_map.get(ind).add(rep_url);
					ind+=1;
					if(ind>count){ind=1;}

				}
			}

		} catch (NullPointerException e) {
			System.out.println("NullpointerException caught");

		}
		// shoud it be retuning something else ? - chirs
		return shard_map;

}

public Integer assignOneNodeToShard(){
	if(this.shard_count<1){
		return 0;
	}else if(this.shard_count==1){
		return 1;
	}else{//when shard_count >=2, find the shard with the least members and add replica to this shard
		int minim=1;
		for(int i=2;i<=this.shard_count;i++) {
			if (this.shard_map.get(i).size() < this.shard_map.get(minim).size()) {
				minim = i;
			}
		}
		this.shard_map.get(minim).add(this.getSocketAddress());
		return minim;
	}

}



public boolean reshard(String shard_count){

	if(this.view.size()<2*Integer.parseInt(shard_count)){return false;}

	HashMap<String, Object> tem_store = new HashMap<>();

	for(int i=1;i<=this.shard_count;i++){
		tem_store.putAll(getkvPairsFrom(this.shard_map.get(i).get(0)));
	}

	for(URL url:this.view){
		//TODO: set kvstore to empty
	}

	this.shard_count = Integer.parseInt(shard_count);

	this.shard_map = assignNodesToShard(this.view,this.shard_count);


	for (URL target: this.view){
		if(target.equals(this.socketAddress)){
			continue;
		}else if(!sendShardMapTo(target,this.shard_map)){
			return false;
		}
	}

	for(String key:tem_store.keySet()){

		int hash = Hash.hash32(key.getBytes());
		int target_shard = Hash.unsignedReminaderPlusOne(hash, this.shard_count);

		if(target_shard == this.shard_id){
			this.kvStore.values.put(key,tem_store.get(key));

		}else{
			Map<String, Object> jsonData = new HashMap<>();
			jsonData.put(key,tem_store.get(key));

			Response resp = forwardShardPUT(target_shard, key, jsonData, this.socketAddress);

		}


	}

	return true;

	}

public HashMap<String,Object> getkvPairsFrom(URL url){

	URL endpoint = appURL.addPath(url, "/retrieve-key-value-store");

	RestTemplate restTemplate = new RestTemplate();
	((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
	((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);


	HttpHeaders headers = new HttpHeaders();
	headers.setContentType(MediaType.APPLICATION_JSON);

	HttpEntity<String> request = new HttpEntity<String>("", headers);

	ResponseEntity<String> resp =null;

	try {
		resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("GET"), request, String.class);
	} catch (RestClientException e) {
		System.out.print("Exception" + e.toString());

	}

	JsonElement je = new Gson().fromJson(resp.getBody(), JsonElement.class);
	JsonObject jo = je.getAsJsonObject();

	System.out.println("JSON OBJECT: " + jo.toString());

	JsonObject valuesJson = jo.getAsJsonObject("values");
	Type stringObjMap = new TypeToken<Map<String, Object>>(){}.getType();
	HashMap<String,Object> valuesMap = new Gson().fromJson(valuesJson.toString(), stringObjMap);

	return valuesMap;

}

public String getMembersOfID(String id){

	int id_int = Integer.parseInt(id);
	StringBuffer members_StrBuff = new StringBuffer();


	for(URL url:shard_map.get(id_int)){
		members_StrBuff.append(url.toString().substring(7));
		members_StrBuff.append(',');
	}

	String shard_id_members_Str = members_StrBuff.toString();

	return shard_id_members_Str.substring(0,shard_id_members_Str.length()-1);
}

public boolean addToShard(String id, String url_str){

	Integer id_int=Integer.parseInt(id);
	this.shard_map.get(id_int).add(appURL.processURL(url_str));

	for (URL target: this.view){
		if(target.equals(this.socketAddress)){
			continue;
		}else if(!sendShardMapTo(target,this.shard_map)){
			return false;
		}
	}

	setShardIDTo(url_str,id);

	return true;

}

public void setShardIDTo(String url_str, String id){

	RestTemplate restTemplate = new RestTemplate();
	((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
	((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

	URL endpoint = appURL.addPath(appURL.processURL(url_str), "/key-value-store-shard/set-shard-id");

	JsonObject jobj = new JsonObject();
	jobj.addProperty("ID",id);

	String body = jobj.toString();

	HttpHeaders headers = new HttpHeaders();
	headers.setContentType(MediaType.APPLICATION_JSON);

	HttpEntity<String> request = new HttpEntity<String>(body, headers);

	ResponseEntity<String> resp;

	try {
		resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("PUT"), request, String.class);
	} catch (RestClientException e) {
		System.out.print("Exception" + e.toString());
		return;
	}

	return;


}

public boolean addNewNodeToView(HashSet<URL> target_set, String url){

	for(URL target_url:target_set){

		if(target_url.equals(appURL.processURL(url))){
			continue;
		}else{
			URL end_url = appURL.processURL(url);

			URL endpoint = appURL.addPath(end_url, "/key-value-store-view");

			RestTemplate restTemplate = new RestTemplate();
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

			JsonObject jsonObject = new JsonObject();

			jsonObject.addProperty("socket-address",url);

			String body = jsonObject.toString();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);

			HttpEntity<String> request = new HttpEntity<String>(body, headers);

			ResponseEntity<String> resp =null;

			try {
				resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("PUT"), request, String.class);
			} catch (RestClientException e) {
				System.out.print("Exception" + e.toString());

			}


		}
	}
	return true;


}



public boolean sendShardMapTo(URL target, HashMap<Integer,ArrayList<URL>> shard_map){


	RestTemplate restTemplate = new RestTemplate();
	((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
	((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

	URL endpoint = appURL.addPath(target, "/key-value-store-shard/set-shard-map");

	Gson gson_object = new Gson();
	JsonObject jsonObject = gson_object.toJsonTree(shard_map).getAsJsonObject();

	String body = jsonObject.toString();

	HttpHeaders headers = new HttpHeaders();
	headers.setContentType(MediaType.APPLICATION_JSON);

	HttpEntity<String> request = new HttpEntity<String>(body, headers);

	ResponseEntity<String> resp;

	try {
		resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("PUT"), request, String.class);
	} catch (RestClientException e) {
		System.out.print("Exception" + e.toString());
		return false;
	}

	return true;
}

public HashMap<Integer,ArrayList<URL>> getShardMapFrom(HashSet<URL> target_urls){

	HashMap<Integer,ArrayList<URL>> return_shard_map = new HashMap<>();

	for(URL target_url:target_urls){
		try{
			//TO DO
			RestTemplate restTemplate = new RestTemplate();
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
			((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

			URL endpoint = appURL.addPath(target_url, "/key-value-store-shard/get-shard-map");

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);

			HttpEntity<String> request = new HttpEntity<String>(headers);

			ResponseEntity<HashMap> resp;

			try {
				resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("GET"), request, HashMap.class);
				HashMap<String, Object> unconverted = resp.getBody();
				HashMap<Integer, ArrayList<URL>> fixed_shard_map = new HashMap<Integer, ArrayList<URL>>();
				for (String id: unconverted.keySet()) {
					int i_id = Integer.parseInt(id);
					ArrayList<URL> shard_urls = new ArrayList<URL>();
					for (String u: (ArrayList<String>) unconverted.get(id)) {
						shard_urls.add(appURL.processURL(u));
					}
					fixed_shard_map.put(i_id, shard_urls);
				}
				return fixed_shard_map;

			} catch (RestClientException e) {

				System.out.println("resulted in RestClientException");
				//returnMap.put(target_replica_url, 503);
			}

			if(!return_shard_map.isEmpty()){
				return return_shard_map;
			}

		}catch (Exception e){
			System.out.print("exception");
		}

	}

	return return_shard_map;
}


public Response setShardMap(Map<String, Object> jsonData){

	System.out.println(this.socketAddress.toString() + ": RESHADRING KEY MAP");
	System.out.println("RECIEVED:" + new Gson().toJson(jsonData).toString());

	this.shard_map = new HashMap<Integer, ArrayList<URL>>();
	for (String mod: jsonData.keySet()) {
		ArrayList<String> temp_shards = (ArrayList<String>) jsonData.get(mod);
		ArrayList<URL> shard_urls = new ArrayList<URL>();
		for (String u: temp_shards) {
			System.out.println(u);
			shard_urls.add(appURL.processURL(u));
		}
		int thisShardId = Integer.parseInt(mod);
		this.shard_map.put(thisShardId, shard_urls);
		if (this.shard_map.get(thisShardId).contains(this.socketAddress)) {
			this.shard_id = thisShardId;
			this.shard_count = this.shard_map.get(this.shard_id).size();
		}
	}

	System.out.println("SHARD ID: " + this.shard_id);
	System.out.println("SHARD COUNT: " + this.shard_count);

	return Response.createJsonResponse(200,
			Response.pair("message", "Set shard map Successfully"));

}

public Response setShardID(String id_str){
	this.shard_id = Integer.parseInt(id_str);
	return Response.createJsonResponse(200,
			Response.pair("message", "Set shard ID Successfully"));

}


}
