package app;

import java.net.URL;
import java.util.HashMap;
import java.util.ArrayList;


import org.springframework.web.client.RestTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

import org.springframework.http.HttpMethod;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;


public class Broadcast {

// args
// replica to be broadcast from
// requestType "DELETE" "PUT" "GET" ect,
// path the request path /key-value-store-view
// body as string, probably formatted as JSON
public static HashMap<URL, Integer> send(Replica replica, String requestType, String path, String body){
	Application.heartbeat_enable.getAndIncrement();

	HashMap<URL, Integer> returnMap = new HashMap<URL, Integer>();   // return map to hold the response of each broadcast
	try {
		for (URL target_replica_url : replica.view) {
			if (!target_replica_url.equals(replica.getSocketAddress())) {
				RestTemplate restTemplate = new RestTemplate();
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
				((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

				URL endpoint = appURL.addPath(target_replica_url, path);
				System.out.println("ENDPOINT: " + endpoint.toString());

				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);

				HttpEntity<String> request = new HttpEntity<String>(body, headers);

				System.out.println("Broadcasting " + requestType + " to " + target_replica_url.toString() + path);
				ResponseEntity<String> resp;
				try {
					resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf(requestType), request, String.class);
					returnMap.put(target_replica_url, resp.getStatusCodeValue());
				} catch (RestClientException e) {
					System.out.println("Broadcast to " + target_replica_url.toString() +
														 " resulted in RestClientException");
					returnMap.put(target_replica_url, 503);
					continue;
				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION: " + e.toString());
	}
	Application.heartbeat_enable.getAndDecrement();
	return returnMap;
}


public static HashMap<URL, Integer> send(Replica replica, String requestType, String path, String body, URL sender_url){
	Application.heartbeat_enable.getAndIncrement();

	HashMap<URL, Integer> returnMap = new HashMap<URL, Integer>();   // return map to hold the response of each broadcast
	try{
		if (!replica.replicaExistsInView(sender_url)) {   // Only broadcast if the client is not a replica

			for (URL target_replica_url : replica.view) {

				if (!target_replica_url.equals(replica.getSocketAddress()) && !target_replica_url.equals(sender_url)) {

					RestTemplate restTemplate = new RestTemplate();
					((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
					((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

					URL endpoint = appURL.addPath(target_replica_url, path);
					System.out.println("ENDPOINT: " + endpoint.toString());

					HttpHeaders headers = new HttpHeaders();
					headers.setContentType(MediaType.APPLICATION_JSON);

					HttpEntity<String> request = new HttpEntity<String>(body, headers);

					System.out.println("Broadcasting " + requestType + " to " + target_replica_url.toString() + path);
					ResponseEntity<String> resp;

	      	try {

						resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf(requestType), request, String.class);
						returnMap.put(target_replica_url, resp.getStatusCodeValue());

	        } catch (RestClientException e) {

						System.out.println("Broadcast to " + target_replica_url.toString() +
															 " resulted in RestClientException: " + e.getMessage());
						returnMap.put(target_replica_url, 503);
						continue;
					}
				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION: " + e.toString());
	}
	Application.heartbeat_enable.getAndDecrement();
	return returnMap;
}

public static HashMap<URL, Integer> sendToShards(Replica replica, String requestType, String path, String body, URL sender_url){
	Application.heartbeat_enable.getAndIncrement();
	ArrayList<URL> target_list = replica.shard_map.get(replica.shard_id);
	HashMap<URL, Integer> returnMap = new HashMap<URL, Integer>();   // return map to hold the response of each broadcast
	try{
		if (!replica.replicaExistsInView(sender_url)) {   // Only broadcast if the client is not a replica

			for (URL target_replica_url : replica.view) {
				if (target_list.contains(target_replica_url) && !target_replica_url.equals(replica.getSocketAddress())) {

					RestTemplate restTemplate = new RestTemplate();
					((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
					((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

					URL endpoint = appURL.addPath(target_replica_url, path);
					System.out.println("ENDPOINT: " + endpoint.toString());

					HttpHeaders headers = new HttpHeaders();
					headers.setContentType(MediaType.APPLICATION_JSON);

					HttpEntity<String> request = new HttpEntity<String>(body, headers);

					System.out.println("Broadcasting " + requestType + " to " + target_replica_url.toString() + path);
					ResponseEntity<String> resp;

	      	try {

						resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf(requestType), request, String.class);
						returnMap.put(target_replica_url, resp.getStatusCodeValue());

	        } catch (RestClientException e) {

						System.out.println("Broadcast to " + target_replica_url.toString() +
															 " resulted in RestClientException");
						returnMap.put(target_replica_url, 503);
						continue;
					}

				} else if (!target_replica_url.equals(replica.getSocketAddress()) && !target_replica_url.equals(sender_url)) {


					RestTemplate restTemplate = new RestTemplate();
					((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setConnectTimeout(2000);
					((SimpleClientHttpRequestFactory)restTemplate.getRequestFactory()).setReadTimeout(2000);

					URL endpoint = appURL.addPath(target_replica_url, "/notify-metadata");
					System.out.println("ENDPOINT: " + endpoint.toString());

					HttpHeaders headers = new HttpHeaders();
					headers.setContentType(MediaType.APPLICATION_JSON);

					HttpEntity<String> request = new HttpEntity<String>(body, headers);

					System.out.println("Broadcasting " + requestType + " to " + target_replica_url.toString() + path);
					ResponseEntity<String> resp;

					try {

						resp = restTemplate.exchange(endpoint.toString(), HttpMethod.valueOf("PUT"), request, String.class);
						returnMap.put(target_replica_url, resp.getStatusCodeValue());

					} catch (RestClientException e) {

						System.out.println("Broadcast to " + target_replica_url.toString() +
															 " resulted in RestClientException");
						returnMap.put(target_replica_url, 503);
						continue;
					}

				}
			}
		}
	} catch (Exception e) {
		System.out.println("CAUGHT SOME OTHER EXCEPTION: " + e.toString());
	}
	Application.heartbeat_enable.getAndDecrement();
	return returnMap;
}

}
