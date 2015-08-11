package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

public class IxS3FileSystem extends FileSystem {
    private Path workingDir;
    private NativeS3FileSystem nativeFS;
    private URI uri;
    
    public IxS3FileSystem(NativeS3FileSystem fileSystem) {
        nativeFS = fileSystem;
    }

    public IxS3FileSystem() {
        nativeFS = new NativeS3FileSystem();
    }

    @Override
    public URI getUri() {
        //Change this
        return this.uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        this.uri = uri;
        nativeFS.initialize(IxS3Path.pickRandomPath(new Path(uri)).toUri(), conf);
        setConf(conf);
        super.initialize(uri, conf);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        List<Path> paths = existingPathsFor(absolute(path));
        if(paths.isEmpty()) throw new IOException(path +" doesnt exist");
        return nativeFS.open(paths.get(0), bufferSize);
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
            delete(path, true);
        }
        return nativeFS.create(IxS3Path.pickRandomPath(absolute(path)), fsPermission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new IOException("Not supported in NativeS3FileSystem, since optional");
    }

    @Override
    public boolean rename(Path from, Path to) throws IOException {
        if(!exists(from)) throw new IOException(from + " doesnt exist");
        for (Path path : existingPathsFor(absolute(from))) {
            nativeFS.rename(path, IxS3Path.withSamePrefix(path, to));
        }
        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        boolean exist = false;
        for (Path p : existingPathsFor(absolute(path))) {
            exist = nativeFS.delete(p, recursive);
        }
        return exist;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        Map<Path,FileStatus> allStatuses = new TreeMap<Path,FileStatus>();
        for (Path p : existingPathsFor(absolute(path))) {
            for (FileStatus fileStatus : nativeFS.listStatus(p)) {
                if(allStatuses.containsKey(fileStatus.getPath()))
                    allStatuses.put(fileStatus.getPath(), merge(allStatuses.get(fileStatus.getPath()), convertToIxPath(fileStatus)));
                else
                    allStatuses.put(fileStatus.getPath(), convertToIxPath(fileStatus));
            }
        }
        return allStatuses.values().toArray(new FileStatus[0]);
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
        if(existing.isEmpty()) return nativeFS.mkdirs(IxS3Path.pickRandomPath(absolute(path)), fsPermission);
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        List<Path> existing = existingPathsFor(absolute(path));
        if(existing.isEmpty()) return convertToIxPath(nativeFS.getFileStatus(IxS3Path.pickRandomPath(absolute(path))));
        FileStatus fileStatus = null;
        for (Path p : existing) {
            if(fileStatus == null) fileStatus = convertToIxPath(nativeFS.getFileStatus(p));
            else fileStatus = merge(fileStatus, convertToIxPath(nativeFS.getFileStatus(p)));
        }
        return fileStatus;
    }

    @Override
    public void close() throws IOException {
        nativeFS.close();
        super.close();
    }

    private List<Path> existingPathsFor(Path path) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        for (Path p : IxS3Path.allPaths(path)) {
            if(nativeFS.exists(p)) paths.add(p);
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

    private static FileStatus convertToIxPath(FileStatus status) {
        return new FileStatus(
                status.getLen(),
                status.isDirectory(),
                status.getReplication(),
                status.getBlockSize(),
                status.getModificationTime(),
                status.getAccessTime(),
                status.getPermission(),
                status.getOwner(),
                status.getGroup(),
                IxS3Path.toIxS3Path(status.getPath())
        );
    }
}
