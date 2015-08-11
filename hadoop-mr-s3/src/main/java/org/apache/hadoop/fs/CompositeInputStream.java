package org.apache.hadoop.fs;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class CompositeInputStream extends InputStream {
    private final InputStream[] streams;
    private int current;

    public CompositeInputStream(InputStream[] streams) {
        this.current = 0;
        this.streams = streams;
    }

    public CompositeInputStream(List<? extends InputStream> streams){
        this(streams.toArray(new InputStream[0]));
    }


    @Override
    public int read() throws IOException {
        if(current >= streams.length) return -1;
        int data = this.streams[current].read();
        if(data != -1) return data;
        current++;
        return this.read();
    }

    @Override
    public void close() throws IOException {
        List<Throwable> exceptions = new ArrayList<Throwable>();
        for (InputStream stream : streams) {
            try {
                stream.close();
            }catch (Exception e){
                exceptions.add(e);
            }
        }
        IxInputStreamException.raise(exceptions);
    }
}
