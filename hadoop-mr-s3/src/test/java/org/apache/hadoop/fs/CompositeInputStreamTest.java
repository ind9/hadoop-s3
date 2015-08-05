package org.apache.hadoop.fs;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class CompositeInputStreamTest {
    @Test
    public void shouldComposeAInputStreamThatReadsOneByOneFromAListOfStreams() throws IOException {
        List<ByteArrayInputStream> streams = Arrays.asList(
                new ByteArrayInputStream("hello ".getBytes()),
                new ByteArrayInputStream("world".getBytes()));

        assertThat(IOUtils.toString(new CompositeInputStream(streams)),is("hello world"));
    }

    @Test
    public void shouldCloseAllStreamsInCaseTheCompositeStreamIsClosed() throws IOException {
        List<InputStream> streams = Arrays.asList(
                mock(InputStream.class),
                mock(InputStream.class)
        );
        new CompositeInputStream(streams).close();

        verify(streams.get(0)).close();
        verify(streams.get(1)).close();
    }

    @Test
    public void shouldCloseOtherStreamsEvenInCaseTheStreamThrowsAnException() throws IOException {
        List<InputStream> streams = Arrays.asList(
                mock(InputStream.class),
                mock(InputStream.class)
        );
        doThrow(new IOException("Something went wrong with stream")).when(streams.get(0)).close();
        try {
            new CompositeInputStream(streams).close();
        }catch (IxInputStreamException e){}

        verify(streams.get(0)).close();
        verify(streams.get(1)).close();
    }
}