package com.membase.jtap;

import com.membase.jtap.ops.BackfillStream;
import com.membase.jtap.ops.DumpStream;
import com.membase.jtap.ops.KeysOnlyStream;
import com.membase.jtap.ops.ListVBucketsStream;
import com.membase.jtap.ops.TapStream;

public class TapRunner {
	public static void main(String args[]) {
		int[] vbucketlist = {1, 2, 1002};
		TapStream tapListener = new DumpStream("default", null, "node1");
		//TapStream tapListener = new BackfillStream("default", null, "node1");
		//TapStreamConfig tapListener = new KeysOnlyStreamConfig("default", null, "node1");
		//TapStreamConfig tapListener = new ListVBucketsStreamConfig("default", null, "node1", vbucketlist);
		//TapStreamClient client = new TapStreamClient("10.2.1.11", 11210, "saslbucket", "password");
		TapStreamClient client = new TapStreamClient("10.2.1.11", 11210, "default", null);
		client.start(tapListener);
		/*try {
			Thread.sleep(180000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client.stop();*/
	}
}
