package org.apache.hadoop.fs;

import org.junit.Test;

import java.util.List;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;


public class IxS3PathTest {
    @Test
    public void shouldCreateS3NativePathsWithEveryOneOfThePrefixes() {
        List<Path> paths = IxS3Path.allPaths(new Path("s3i://somebucket/somedirectory/someotherdirectory/somefile"));
        assertThat(paths.size(), is(36));
        assertThat(paths.get(0), is(new Path("s3n://somebucket/0/somedirectory/someotherdirectory/somefile")));
    }

    @Test
    public void shouldCreateS3NativePathWithRandomPrefix() {
        Path iPath = new Path("s3i://somebucket/somedirectory/someotherdirectory/somefile");
        Path path = IxS3Path.pickRandomPath(iPath);
        assertThat(path, isIn(IxS3Path.allPaths(iPath)));
    }

    @Test
    public void shouldCreateIxS3PathFromNativePathByChangingSchemeAndRemovingThePrefix() {
        Path nativePath = new Path("s3n://somebucket/p/somedirectory");
        assertThat(IxS3Path.toIxS3Path(nativePath), is(new Path("s3i://somebucket/somedirectory")));
    }

    @Test
    public void shouldFindANewNativePathWithSamePrefixButDifferentFileOrDirectory() {
        Path s3Path = IxS3Path.withSamePrefix(new Path("s3n://somebucket/p/somedirectory"), new Path("s3i://somebucket/someotherdirectory"));
        assertThat(s3Path, is(new Path("s3n://somebucket/p/someotherdirectory")));
    }
}
