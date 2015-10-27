package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.Jets3tNFSStoreWithStorageClassSupport;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * Wrapper over {@see NativeS3FileSystem} to provide S3 storage class support via fs.s3n.storage.class
 * configuration property.
 */
public class IxS3NativeFileSystemWithStorageClass extends FileSystem {
    private Path workingDir;
    private NativeS3FileSystem nativeFS;
    private URI uri;

    public IxS3NativeFileSystemWithStorageClass(NativeS3FileSystem fileSystem) {
        nativeFS = fileSystem;
    }

    public IxS3NativeFileSystemWithStorageClass() {
        nativeFS = new NativeS3FileSystem(new Jets3tNFSStoreWithStorageClassSupport());
    }

    @Override
    public URI getUri() {
        //Change this
        return this.uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        this.uri = uri;
        nativeFS.initialize(uri, conf);
        setConf(conf);
        super.initialize(uri, conf);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return nativeFS.open(path, bufferSize);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return nativeFS.exists(path);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        return nativeFS.create(path, fsPermission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new IOException("Not supported in NativeS3FileSystem, since optional");
    }

    @Override
    public boolean rename(Path from, Path to) throws IOException {
        return nativeFS.rename(from, to);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return nativeFS.delete(path, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return nativeFS.listStatus(path);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        this.workingDir = path;
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return nativeFS.mkdirs(path, fsPermission);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return nativeFS.getFileStatus(path);
    }

    @Override
    public void close() throws IOException {
        nativeFS.close();
        super.close();
    }
}
