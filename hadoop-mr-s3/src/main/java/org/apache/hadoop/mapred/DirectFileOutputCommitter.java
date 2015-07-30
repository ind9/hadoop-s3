package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;

import java.io.IOException;


/*
* Realized that DFOC doesn't need anything specific. In other words, it's a NOOP version of FOC
* */
public class DirectFileOutputCommitter extends FileOutputCommitter{
    private static final Log LOG = LogFactory.getLog(DirectFileOutputCommitter.class);

    public DirectFileOutputCommitter() {
    }

    public void setupJob(JobContext context) throws IOException {
        if(this.directWriteEnabled((JobContext) context)) {
            LOG.info("Nothing to setup for DFOC");
        } else {
            super.setupJob(context);
        }

    }

    public void cleanupJob(JobContext context) throws IOException {
        if(this.directWriteEnabled((JobContext) context)) {
            LOG.info("Nothing to clean up for DFOC");
        } else {
            super.cleanupJob(context);
        }

    }

    public void setupTask(TaskAttemptContext context) throws IOException {
        if(!this.directWriteEnabled((TaskAttemptContext) context)) {
            super.setupTask(context);
        }

    }

    public void commitTask(TaskAttemptContext context) throws IOException {
        if(this.directWriteEnabled((TaskAttemptContext) context)) {
            LOG.info("This is a NOOP for DFOC");
        } else {
            super.commitTask(context);
        }

    }

    /*TODO: Should we cleanup partial files during job abort? */
    public void abortTask(TaskAttemptContext context) throws IOException {
        if(this.directWriteEnabled((TaskAttemptContext) context)) {
            LOG.info("Nothing to clean up on abort for DFOC");
        } else {
            super.abortTask(context);
        }

    }

    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return this.directWriteEnabled((TaskAttemptContext) context) ? false : super.needsTaskCommit(context);
    }

    public Path getWorkPath(TaskAttemptContext taskContext, Path basePath) throws IOException {
        return this.directWriteEnabled((TaskAttemptContext) taskContext)?FileOutputFormat.getOutputPath(taskContext.getJobConf()):super.getWorkPath(taskContext, basePath);
    }

    private boolean directWriteEnabled(TaskAttemptContext c) throws IOException {
        return this.directWriteEnabled((JobConf) c.getJobConf());
    }

    private boolean directWriteEnabled(JobContext jc) throws IOException {
        return this.directWriteEnabled((JobConf) jc.getJobConf());
    }

    private boolean directWriteEnabled(JobConf conf) throws IOException {
        Path p;
        if((p = FileOutputFormat.getOutputPath(conf)) == null) {
            return false;
        } else {
            return p.getFileSystem(conf) instanceof NativeS3FileSystem;
        }
    }
}
