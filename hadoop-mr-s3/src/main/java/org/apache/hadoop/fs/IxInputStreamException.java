package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.List;

public class IxInputStreamException  extends IOException {
    private final List<? extends Throwable> exceptions;

    public IxInputStreamException(List<? extends Throwable> exceptions) {
        this.exceptions = exceptions;
    }

    public static void raise(List<? extends Throwable> exceptions) throws IxInputStreamException {
        if(exceptions.isEmpty()) return;
        throw new IxInputStreamException(exceptions);
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        return exceptions.get(0).getStackTrace();
    }
}
