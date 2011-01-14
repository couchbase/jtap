package com.membase.nodecode.tap;

import com.membase.nodecode.tap.internal.TapStreamClient;
import com.membase.nodecode.tap.ops.DumpStream;
import com.membase.nodecode.tap.ops.TapStream;

public class TapRunner {
	public static void main(String args[]) {
		boolean[] prepare = {false};
	    boolean[] cleanup = {false};
	    int[] count = {0};
		TapStream tapListener = new DumpStream("default", null, "node1");
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
