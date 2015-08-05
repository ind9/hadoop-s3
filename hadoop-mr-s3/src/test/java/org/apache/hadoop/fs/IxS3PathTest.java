package org.apache.hadoop.fs;

import org.junit.Test;

import java.net.URISyntaxException;
import java.util.List;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;


public class IxS3PathTest {
    @Test
    public void shouldCreateS3NativePathsWithEveryOneOfThePrefixes() throws URISyntaxException {
        List<Path> paths = IxS3Path.allPaths(new Path("s3i://somebucket/somedirectory/someotherdirectory/somefile"));
        assertThat(paths.size(), is(36));
        assertThat(paths.get(0), is(new Path("s3n://somebucket/0/somedirectory/someotherdirectory/somefile")));
    }

    @Test
    public void shouldCreateS3NativePathWithRandomPrefix() throws URISyntaxException {
        Path iPath = new Path("s3i://somebucket/somedirectory/someotherdirectory/somefile");
        Path path = IxS3Path.pickRandomPath(iPath);
        assertThat(path, isIn(IxS3Path.allPaths(iPath)));
    }
}
