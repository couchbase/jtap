package com.membase.nodecode.tap;

import com.membase.nodecode.tap.internal.TapStreamClient;
import com.membase.nodecode.tap.ops.DumpStreamConfig;
import com.membase.nodecode.tap.ops.KeysOnlyStreamConfig;
import com.membase.nodecode.tap.ops.TapStreamConfig;

public class TapRunner {
	public static void main(String args[]) {
		TapStreamConfig tapListener = new DumpStreamConfig("default", null, "node1");
		//TapStreamConfig tapListener = new KeysOnlyStreamConfig("default", null, "node1");
		TapStreamClient client = new TapStreamClient("10.2.1.11", 11210);
		client.start(tapListener);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client.stop();
	    
	}
}
