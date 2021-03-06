----------------------------------------------------------------------
                           __  __
                          |__|/  |______  ______
                          |  \   __\__  \ \____ \
                          |  ||  |  / __ \|  |_> >
                      /\__|  ||__| (____  /   __/
                      \______|          \/|__|

----------------------------------------------------------------------
                        A Tap Client for Java
----------------------------------------------------------------------


# Building

    ant

A jar should be generated in the build folder

# Documentation

    ant doc

Check out docs/javadocs/index.html

# Using

You need to create a `TapStreamClient`, an `Exporter`, and a
`TapStream`. The `TapStreamClient` is given an exporter which handles
data output from the tap stream connection. To start streaming data
call the start function in `TapStreamClient` and pass in a `TapStream`
template.

# Example

Creates a custom tap stream that will dump all key in a Membase server
and send only the key names. All of the key names will be exported to
a file named `results.txt`

    TapStreamClient client = new TapStreamClient("10.1.5.102", 11210, "default", null);
    Exporter exporter = new FileExporter("results.txt");
    CustomStream tapListener = new CustomStream(exporter, "node1");
    tapListener.keysOnly();
    tapListener.doDump();
    client.start(tapListener);

# Contact

The [couchbase server forums][forum] welcomes you and whatever
questions you may have regarding using jtap and all other things
couchbase.

See also: the full specification for the [TAP protocol][tapspec].

[tapspec]: http://techzone.couchbase.com/wiki/display/membase/TAP+Protocol
[forum]: http://techzone.couchbase.com/forums/couchbase/couchbase-server
