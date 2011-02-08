package com.membase.jtap;

import org.apache.http.nio.reactor.IOReactorException;

import com.membase.jtap.exporter.Exporter;
import com.membase.jtap.exporter.FileExporter;
import com.membase.jtap.exporter.SolrExporter;
import com.membase.jtap.exporter.TextExporter;
import com.membase.jtap.ops.BackfillStream;
import com.membase.jtap.ops.CustomStream;
import com.membase.jtap.ops.DumpStream;
import com.membase.jtap.ops.KeysOnlyStream;
import com.membase.jtap.ops.ListVBucketsStream;
import com.membase.jtap.ops.TapStream;

public class TapRunner {
	public static void main(String args[]) {
		int[] vbucketlist = {1, 0};
		//Exporter exporter = new FileExporter("results");
		//Exporter exporter = new TextExporter();
		Exporter exporter = null;
		try {
			exporter = new SolrExporter("http://10.2.1.11:8983");
		} catch (IOReactorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//TapStream tapListener = new DumpStream(exporter, "node1");
		//TapStream tapListener = new BackfillStream(exporter, "node1", null);
		/*try {
			exporter.write("key1");
			exporter.write("key2");
			Thread.sleep(10000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);*/
		CustomStream tapListener = new CustomStream(exporter, "node1");
		tapListener.keysOnly();
		tapListener.doDump();
		//tapListener.specifyVbuckets(vbucketlist);
		TapStreamClient client = new TapStreamClient("10.1.5.102", 11210, "default", null);
		//TapStreamClient client = new TapStreamClient("10.2.1.11", 11210, "default", null);
		client.start(tapListener);

		// No arrays
		// Don't pass tapStream in start
		
		/*try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		client.stop();*/
	}
}
