package app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.stream.Collectors;


import java.util.UUID;
import java.lang.StringBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;



public class KeyValueStore {

	public HashMap<String, Object> values;
	public HashMap<String, String> version;
	public LinkedList<String> orderedVersion;

	public KeyValueStore() {

		this.values = new HashMap<String, Object>();
		this.version = new HashMap<String, String>();
		this.orderedVersion = new LinkedList<String>();
	}

	public int size()  {
		return this.values.keySet().size();
	}

	public boolean kvStoreisEmpty() {
		return this.size() == 0;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (String key: values.keySet()) {
			sb.append(key);
			sb.append(" = ");
			sb.append(this.values.get(key));
			sb.append(" (");
			sb.append(this.version.get(key));
			sb.append(")\n");
		}
		sb.append("=========");
		sb.append("versions:\n");
		for (String s: this.orderedVersion) {
			sb.append(s);
			sb.append("\n");
		}
		return sb.toString();
	}

	public String toJsonString() {
		String valuesJsonStr = new Gson().toJson(this.values);
		String versionJsonStr = new Gson().toJson(this.version);

		JsonObject valuesJson = new Gson().fromJson(valuesJsonStr, JsonObject.class);
		JsonObject versionJson = new Gson().fromJson(versionJsonStr, JsonObject.class);

		JsonArray orderedJson = new JsonArray();
		for (String entry : this.orderedVersion) {
			orderedJson.add(entry);
		}
		JsonObject complete = new JsonObject();
		complete.add("values", valuesJson);
		complete.add("version", versionJson);
		complete.add("orderedVersion", orderedJson);
		return complete.toString();
	}

	public JsonObject toJsonObject() {
		String valuesJsonStr = new Gson().toJson(this.values);
		String versionJsonStr = new Gson().toJson(this.version);

		JsonObject valuesJson = new Gson().fromJson(valuesJsonStr, JsonObject.class);
		JsonObject versionJson = new Gson().fromJson(versionJsonStr, JsonObject.class);

		JsonArray orderedJson = new JsonArray();
		for (String entry : this.orderedVersion) {
			orderedJson.add(entry);
		}
		JsonObject complete = new JsonObject();
		complete.add("values", valuesJson);
		complete.add("version", versionJson);
		complete.add("orderedVersion", orderedJson);
		return complete;
	}

	public String generateUUID() {
		String generatedUUID = UUID.randomUUID().toString();
		if (! this.version.containsValue(generatedUUID)) {
			return generatedUUID;
		} else {
			return this.generateUUID();
		}
	}

	// returns 'true' if the key exists in the kv-store, 'false' otherwise
	public boolean keyExists(String key) {
		return this.values.containsKey(key);
	}

	// returns the current version for a key
	public String versionForKey(String key) {
		return this.version.getOrDefault(key, null);
	}

	public boolean versionExisted(String version) {
		return this.orderedVersion.contains(version);
	}

	// return an ArrayList of keys this depends on
	// with the most important first.
	public ArrayList<String> versionDependsOn(String version) throws RuntimeException {
		ArrayList<String> temp = new ArrayList<String>();
		if (this.orderedVersion.contains(version)) {
			int indexVer = this.orderedVersion.indexOf(version);
			for (int i = indexVer; i >= 0; i--) {
				temp.add(new String(this.orderedVersion.get(i)));
			}
			return temp;
		}
		throw new RuntimeException("Could not find that version in current history.");
	}
	public Response updateMetadata(String metadata) {
		this.orderedVersion.add(metadata);
		return Response.createJsonResponse(200, Response.pair("success", "okay"));
	}
	public Response put(String key, Object value, String withVersion, int shard_id) {

		// if the key length > 50, return a 400 error

		if (key.length() > 50) {

			return Response.createJsonResponse(
					400,
					Response.pair("error", "Key is too long"),
					Response.pair("message", "Error in PUT")
					);
		}

		boolean keyExistedPreviously = this.keyExists(key);

		if (String.valueOf(value).trim().length() == 0) {
			value = null;
		}

		// doesnt check if this has valid previous metadata, that is done in Replica
		this.values.put(key, value);
		if (withVersion != null) {
			this.version.put(key, withVersion);
			this.orderedVersion.add(withVersion);
		} else {
			this.version.put(key, this.generateUUID());
			this.orderedVersion.add(this.version.get(key));
		}
		System.err.printf(String.format("IN PUT: PUT %s = %s %s\n", key, value, this.version.get(key)));

		String dependsOnStr = "";
		if (this.versionForKey(key) != null) {
			dependsOnStr = this.versionDependsOn(this.version.get(key)).stream().collect(Collectors.joining(","));
		}

		if (keyExistedPreviously) {
			// old key

			return Response.createJsonResponse(
					200,
					Response.pair("message", "Updated successfully"),
					Response.pair("version", this.version.get(key)),
					Response.pair("causal-metadata", dependsOnStr),
					Response.pair("shard-id", Integer.toString(shard_id))
					);
		} else {
			// new key
			return Response.createJsonResponse(
					201,
					Response.pair("message", "Added successfully"),
					Response.pair("version", this.version.get(key)),
					Response.pair("causal-metadata", dependsOnStr),
					Response.pair("shard-id", Integer.toString(shard_id))
					);
			}
		}

	public Response get(String key) {

		// key does not exist
		if (!this.keyExists(key)) {

			return Response.createJsonResponse(
					404,
					Response.pair("error", "Key does not exist"),
					Response.pair("message", "Error in GET")
					);

		// existing key
		// return its latest value


		} else {

			String dependsOnStr = "";
			if (this.versionForKey(key) != null) {
				dependsOnStr = this.versionDependsOn(this.version.get(key)).stream().collect(Collectors.joining(","));
			}

			return Response.createJsonResponse(
					200,
					Response.pair("message", "Retrieved successfully"),
					Response.pair("version", this.version.get(key)),
					Response.pair("causal-metadata", dependsOnStr),
					Response.pair("value", this.values.get(key))
					);
		}
	}

	public Response delete(String key, String withVersion) {

		// key does not exist
		if (!this.keyExists(key)) {

			return Response.createJsonResponse(
					404,
					Response.pair("error", "Key does not exist"),
					Response.pair("message", "Error in DELETE")
					);


		} else {

			this.values.put(key, null);
			if (withVersion != null) {
				this.version.put(key, withVersion);
				this.orderedVersion.add(withVersion);
			} else {
				this.version.put(key, this.generateUUID());
				this.orderedVersion.add(this.version.get(key));
			}
			System.err.printf(String.format("IN DELETE: %s = %s\n", key, this.version.get(key)));

			String dependsOnStr = "";
			if (this.versionForKey(key) != null) {
				dependsOnStr = this.versionDependsOn(this.version.get(key)).stream().collect(Collectors.joining(","));
			}

			Response resp = Response.createJsonResponse(
					200,
					Response.pair("message", "Deleted successfully"),
					Response.pair("version", this.version.get(key)),
					Response.pair("causal-metadata", dependsOnStr)
					);
		return resp;
		}
	}

}
