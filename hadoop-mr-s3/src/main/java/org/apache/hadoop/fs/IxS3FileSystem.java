package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IxS3FileSystem extends NativeS3FileSystem {
    private Path workingDir;

    private List<Path> existingPathsFor(Path path) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        for (Path p : IxS3Path.allPaths(path)) {
            if(super.exists(p)) paths.add(p);
        }
        return paths;
    }

    private Path absolute(Path path) {
        if(path.isAbsolute()) return path;
        return new Path(workingDir, path);
    }

    @Override
    public URI getUri() {
        //Change this
        return super.getUri();
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        List<InputStream> streams = new ArrayList<InputStream>();
        for (Path p : existingPathsFor(absolute(path))) {
            streams.add(super.open(p, bufferSize));
        }
        if(streams.isEmpty()) throw new IOException(path +" doesnt exist");
        return new FSDataInputStream(new CompositeInputStream(streams));
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return !existingPathsFor(absolute(path)).isEmpty();
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        if(exists(path)) {
            if(!overwrite) throw new IOException(path + " already exists");
            delete(path,true);
        }
        return super.create(IxS3Path.pickRandomPath(absolute(path)),fsPermission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new IOException("Not supported in NativeS3FileSystem, since optional");
    }

    @Override
    public boolean rename(Path from, Path to) throws IOException {
        if(!exists(from)) throw new IOException(from + " doesnt exist");
        throw new RuntimeException("Will comeback to this");
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        boolean exist = false;
        for (Path p : existingPathsFor(absolute(path))) {
            exist = super.delete(p, recursive);
        }
        return exist;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        List<FileStatus> allStatuses = new ArrayList<FileStatus>();
        for (Path p : existingPathsFor(absolute(path))) {
            allStatuses.addAll(Arrays.asList(super.listStatus(p)));
        }
        return (FileStatus[]) allStatuses.toArray();
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
        List<Path> existing = existingPathsFor(absolute(path));
        if(existing.isEmpty()) return super.mkdirs(IxS3Path.pickRandomPath(absolute(path)), fsPermission);
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        List<Path> existing = existingPathsFor(absolute(path));
        if(existing.isEmpty()) return super.getFileStatus(IxS3Path.pickRandomPath(absolute(path)));
        return super.getFileStatus(existing.get(0));
    }
}
