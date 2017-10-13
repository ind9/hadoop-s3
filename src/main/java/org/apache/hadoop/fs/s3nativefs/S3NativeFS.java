package org.apache.hadoop.fs.s3nativefs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.util.DataChecksum;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.s3native.S3NativeFileSystemConfigKeys.*;

// Ref - https://issues.apache.org/jira/browse/HADOOP-10643
// Without this, we can't use fs.default.name or fs.defaultFS with s3n
public class S3NativeFS extends DelegateToFileSystem {

    public static FsServerDefaults SERVER_DEFAULTS = new FsServerDefaults(
            S3_NATIVE_BLOCK_SIZE_DEFAULT,
            S3_NATIVE_BYTES_PER_CHECKSUM_DEFAULT,
            S3_NATIVE_CLIENT_WRITE_PACKET_SIZE_DEFAULT,
            S3_NATIVE_REPLICATION_DEFAULT,
            S3_NATIVE_STREAM_BUFFER_SIZE_DEFAULT,
            false,
            FS_TRASH_INTERVAL_DEFAULT,
            DataChecksum.CHECKSUM_NULL);

    public S3NativeFS(URI theUri, Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new NativeS3FileSystem(), conf, "s3n", true);
    }

    @Override
    public int getUriDefaultPort() {
        return -1; // No default port for s3
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return SERVER_DEFAULTS;
    }
}
