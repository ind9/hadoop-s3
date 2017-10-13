package org.apache.hadoop.mapreduce.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

public class IxDirectFileOutputCommitter extends FileOutputCommitter {
    private static final Log LOG = LogFactory.getLog(IxDirectFileOutputCommitter.class);
    private Path outputPath = null;
    private final boolean directWrite;

    public IxDirectFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
        if(outputPath != null && context != null) {
            this.outputPath = outputPath;
            this.directWrite = this.isDirectWrite(context, outputPath);
        } else {
            this.directWrite = false;
        }

    }

    public void setupJob(JobContext context) throws IOException {
        if(this.directWrite) {
            LOG.info("Nothing to setup for DFOC");
        } else {
            super.setupJob(context);
        }

    }

    public void cleanupJob(JobContext context) throws IOException {
        if(this.directWrite) {
            LOG.info("Nothing to cleanup for DFOC");
        } else {
            super.cleanupJob(context);
        }

    }

    public void setupTask(TaskAttemptContext context) throws IOException {
        if(!this.directWrite) {
            super.setupTask(context);
        }

    }

    public void commitTask(TaskAttemptContext context) throws IOException {
        if(this.directWrite) {
            LOG.info("This is a NOOP for DFOC");
        } else {
            super.commitTask(context);
        }

    }

    /*TODO: Should we cleanup partial files during job abort? */
    public void abortTask(TaskAttemptContext context) throws IOException {
        if(this.directWrite) {
            LOG.info("Nothing to clean up on abort for DFOC");
        } else {
            super.abortTask(context);
        }

    }

    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return this.directWrite ? false : super.needsTaskCommit(context);
    }

    public Path getWorkPath() throws IOException {
        return this.directWrite ? this.outputPath : super.getWorkPath();
    }

    private boolean isDirectWrite(JobContext jc, Path outputPath) throws IOException {
        return outputPath.getFileSystem(jc.getConfiguration()) instanceof NativeS3FileSystem;
    }
}
