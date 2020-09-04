package app;

import java.net.MalformedURLException;
import java.net.URL;

public class appURL {

	URL url;

	public appURL(URL u) {
		this.url = u;
	}

	public static URL processURL(String urlString) {

		urlString = urlString.trim();

		// remove remnants depending on where the URL came from
		if (urlString.startsWith("http:")) {
			urlString = urlString.replaceFirst("http:", "");
		} else if (urlString.startsWith("//")) {
			urlString = urlString.replaceFirst("//", "");
		} else if (urlString.startsWith("https:")) {
			urlString = urlString.replaceFirst("https:", "");
		}

		// remove // from the beginning of the URL if it exists
		while (urlString.charAt(0) == '/') {
			urlString = urlString.substring(1);
		}

		// split on port number
		String urlWithScheme = "";
		int lastColonIndex = urlString.lastIndexOf(":");
		int port = 8080;
		// default to 8080 port
		if (lastColonIndex == -1) {
			urlWithScheme = String.format("http://%s:%d", urlString, port);
		// remove port from url with scheme
		} else {
			urlWithScheme = String.format("http://%s", urlString.substring(0, lastColonIndex));
			port = Integer.parseInt(urlString.substring(lastColonIndex + 1));
		}



		URL url = null;

		try {
			// encode url
			url = new URL(urlWithScheme);
			// create url and add port number
			url = new URL(url.getProtocol(), url.getHost(), port, url.getFile());
		} catch (MalformedURLException mfe) {
			mfe.printStackTrace();
			System.exit(1);
		}
		return url;
	}

	public static URL addPath(URL url, String path) {
		URL processedURL = null;
		try {
			processedURL = new URL(url.getProtocol(), url.getHost(), url.getPort(), path);
		} catch (MalformedURLException e) {
			System.err.printf("Failed to process addPath for %s, %s\n", url.toString(), path);
			e.printStackTrace();
		}
		return processedURL;
	}

	public URL getURL() {
		return this.url;
	}

	@Override
	public String toString() {
		return this.url.toString();
	}

}
