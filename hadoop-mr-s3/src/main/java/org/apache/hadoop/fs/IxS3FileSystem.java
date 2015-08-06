package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

public class IxS3FileSystem extends NativeS3FileSystem {
    private Path workingDir;

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
        Map<Path,FileStatus> allStatuses = new TreeMap<Path,FileStatus>();
        for (Path p : existingPathsFor(absolute(path))) {
            for (FileStatus fileStatus : super.listStatus(p)) {
                if(allStatuses.containsKey(fileStatus.getPath()))
                    allStatuses.put(fileStatus.getPath(), merge(allStatuses.get(fileStatus.getPath()), fileStatus));
                else
                    allStatuses.put(fileStatus.getPath(), fileStatus);
            }
        }
        return (FileStatus[]) allStatuses.values().toArray();
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
        FileStatus fileStatus = null;
        for (Path p : existing) {
            if(fileStatus == null) fileStatus = super.getFileStatus(p);
            else fileStatus = merge(fileStatus, super.getFileStatus(p));
        }
        return fileStatus;
    }

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

    private static FileStatus merge(FileStatus first, FileStatus other) {
        return new FileStatus(
                first.getLen() + other.getLen(),
                first.isDirectory(),
                first.getReplication(),
                first.getBlockSize(),
                Math.max(first.getModificationTime(), other.getModificationTime()),
                Math.max(first.getAccessTime(), other.getAccessTime()),
                first.getPermission(),
                first.getOwner(),
                first.getGroup(),
                first.getPath()
        );
    }
}
