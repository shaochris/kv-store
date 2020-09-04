package app;

import java.util.AbstractMap;
import java.util.HashMap;

import org.springframework.http.HttpStatus;

import com.google.gson.Gson;

// Response takes the JSON response string
// and an HTTP code and resolves it to the corresponding
// HttpStatus object

public class Response {

	String responseString;
	HttpStatus httpCode;
	
	
	public Response(String responseString, int httpCode) {
		this.responseString = responseString;
		this.httpCode = HttpStatus.resolve(httpCode);
	}
	
	// create a SimpleEntry pair from a key and value pair
	public static AbstractMap.SimpleEntry<String, Object> pair(String key, Object val) {
		return new AbstractMap.SimpleEntry<String, Object>(key, val);
	}
	
	// create a JSON response from a variable amount of SimpleEntry pairs and a httpCode
	@SafeVarargs
	public static Response createJsonResponse(int httpCode, AbstractMap.SimpleEntry<String, Object>... fields) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		for (AbstractMap.SimpleEntry<String, Object> pair: fields) {
			map.put(pair.getKey(), pair.getValue());
		}
		return new Response(new Gson().toJson(map), httpCode);
	}
}
