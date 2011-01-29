package com.membase.jtap.exporter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.nio.concurrent.FutureCallback;
import org.apache.http.nio.reactor.IOReactorException;

import com.membase.jtap.TapStreamClient;

public class SolrExporter implements Exporter {
	private HttpAsyncClient httpclient;
	private int posts;
	private int totalposts;
	private int completed;
	private String host;

	public SolrExporter(String host) throws IOReactorException {
		this.posts = 0;
		this.totalposts = 0;
		this.completed = 0;
		this.host = host;
		this.httpclient = new DefaultHttpAsyncClient();
		httpclient.start();
	}

	@Override
	public void write(String key) {
		String xml = "<add><doc><field name=\"id\">" + key
				+ "</field></doc></add>";
		try {
			postData(xml);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}

	@Override
	public void write(String key, String value) {
		String xml = "<add><doc><field name=\"id\">" + key
				+ "</field><field name=\"name\">" + value
				+ "</field></doc></add>";
		try {
			postData(xml);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	public void postData(String content) throws Exception {
		HttpPost request;
		HttpEntity entity = new StringEntity(content);
		if (posts < 10000) {
			request = new HttpPost(host + "/solr/update");
			request.setEntity(entity);
			posts++;
		} else {
			totalposts += 10000;
			System.out.println("Posts          : " + totalposts);
			System.out.println("Completed Posts: " + completed);
			request = new HttpPost(host + "/solr/update");
			request.setEntity(new StringEntity("<commit/>"));
			posts = 0;
		}
        httpclient.execute(request, new FutureCallback<HttpResponse>() {
					@Override
					public void cancelled() {
						System.out.println("Cancelled");
					}

					@Override
					public void completed(HttpResponse response) {
						if (response.getStatusLine().getStatusCode() != 200)
							System.out.println(response.getStatusLine());
						completed++;
					}

					@Override
					public void failed(Exception e) {
						System.out.println("Failed: " + e.getMessage());
						System.exit(0);
					}
        });
	}
}
