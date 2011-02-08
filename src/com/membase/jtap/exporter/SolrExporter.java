package com.membase.jtap.exporter;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.nio.concurrent.FutureCallback;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.message.ResponseMessage;

/**
 * The solr exporter is used to export data from tap streams into a solr server. The solr exporter
 * is implemented asynchronously in order to provide the highest throughput possible. The exporter
 * sends rest update requests in xml to the solr server and commits all requests to the server after
 * 15 seconds. 
 */
public class SolrExporter implements Exporter {
	private static final Logger LOG = LoggerFactory.getLogger(SolrExporter.class);
	private static final String KEY = "key";
	private static final String VAL = "value";
	private static final String EXP = "expiration";
	private static final String DEF_KEY = "id";
	private static final String DEF_VAL = "name";
	private static final String DEF_EXP = "weight";
	private static final int COMMIT_TIME = 15000;
	
	private HttpAsyncClient httpclient;
	private long st;
	private long en;
	private int totalposts;
	private int completed;
	private String host;
	private HashMap<String, String> requestFormat;

	/**
	 * Creates a solr exorter.
	 * @param host - the hostname of the solr server.
	 * @throws IOReactorException
	 */
	public SolrExporter(String host) throws IOReactorException {
		this.totalposts = 0;
		this.completed = 0;
		this.host = host;
		this.requestFormat = new HashMap<String, String>();
		this.st = System.currentTimeMillis();
		requestFormat.put(KEY, DEF_KEY);
		requestFormat.put(VAL, DEF_VAL);
		requestFormat.put(EXP, DEF_EXP);
		this.httpclient = new DefaultHttpAsyncClient();
		httpclient.start();
	}

	/**
	 * Writes the contents of a message to the solr server.
	 */
	@Override
	public void write(ResponseMessage message) {
		String key;
		String value;
		try {
			key = message.getKey();
		} catch (FieldDoesNotExistException e) {
			key = null;
			LOG.error("Response doesn't contain a key: " + e.getMessage());
		}
		try {
			value = message.getValue();
		} catch (FieldDoesNotExistException e) {
			value = null;
		}
		String xml = buildXMLRequest(key, value);
		try {
			postData(xml);
		} catch (Exception e) {
			LOG.error("Failed to post data to solr: " + e.getMessage());
		}
	}
	
	/**
	 * Closes the connection to the solr server.
	 */
	@Override
	public void close() {
		try {
			httpclient.shutdown();
		} catch (InterruptedException e) {
			LOG.error("http client interrupted while shutting down");
		}
	}

	private void postData(String content) throws UnsupportedEncodingException  {
		HttpPost request;
		if ((en = System.currentTimeMillis()) - st > COMMIT_TIME) {
			st = en;
			LOG.info(completed + "/" + totalposts + " solr transactions completed");
			request = new HttpPost(host + "/solr/update");
			request.setEntity(new StringEntity("<commit/>"));
			executeRequest(request);
		}
		request = new HttpPost(host + "/solr/update");
		request.setEntity(new StringEntity(content));
		executeRequest(request);
		totalposts++;
	}
       
	
	private void executeRequest(HttpPost request) {
		 httpclient.execute(request, new FutureCallback<HttpResponse>() {
			@Override
			public void cancelled() {
				LOG.error("Solr request cancelled");
			}

			@Override
			public void completed(HttpResponse response) {
				completed++;
			}

			@Override
			public void failed(Exception e) {
				LOG.error("Solr request failed: " + e.getMessage());
			}
 		});
	}
	
	private String buildXMLRequest(String key, String value) {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		String xml = null;
		try {
	    	DocumentBuilder builder = factory.newDocumentBuilder();
	    	DOMImplementation impl = builder.getDOMImplementation();
	    	
		    Document document = impl.createDocument(null,null,null);
		    Element add = document.createElement("add");
		    document.appendChild(add);
		
		    Element doc = document.createElement("doc");
		    add.appendChild(doc);
		    
		    if (key != null)
		    	addFieldElement(document, doc, requestFormat.get(KEY), key);
		    if (value != null)
		    	addFieldElement(document, doc, requestFormat.get(VAL), value);
		    
		    DOMSource domSource = new DOMSource(document);
	        TransformerFactory tf = TransformerFactory.newInstance();
	        Transformer transformer;
	        transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
	        StringWriter sw = new StringWriter();
	        StreamResult sr = new StreamResult(sw);
	        transformer.transform(domSource, sr);
	        xml = sw.toString();
		} catch (ParserConfigurationException e) {
			LOG.error("Couldn't parse xml configuration: " + e.getMessage());	
		} catch (TransformerConfigurationException e) {
			LOG.error("Couldn't configure xml transformer: " + e.getMessage());
		} catch (TransformerException e) {
			LOG.error("Couldn't transform xml statement: " + e.getMessage());
		}
		return xml;	
	}
	
	private void addFieldElement(Document doc, Element parent, String name, String value) {
		Element child = doc.createElement("field");
	    parent.appendChild(child);
	    
	    child.setAttribute("name", name);
	    child.setTextContent(value);
	}
	
	/**
	 * Sets the field name that the key in a tap response message will be written to.
	 * @param id The name of the field to write the key to.
	 */
	public void setKeyFieldName(String id) {
		if (id == null)
			requestFormat.put(KEY, DEF_KEY);
		else
			requestFormat.put(KEY, id);
	}
	
	/**
	 * Sets the field name that the value in a tap response message will be written to.
	 * @param id The name of the field to write value key to.
	 */
	public void setValueFieldName(String id) {
		if (id == null)
			requestFormat.put(VAL, DEF_VAL);
		else
			requestFormat.put(VAL, id);
	}
	
	public void setExpirationFieldName(String id) {
		if (id == null)
			requestFormat.put(EXP, DEF_EXP);
		else
			requestFormat.put(EXP, id);
	}
}
