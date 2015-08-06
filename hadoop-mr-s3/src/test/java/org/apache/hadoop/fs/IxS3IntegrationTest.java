package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class IxS3IntegrationTest {
    public static void main(String[] args) throws URISyntaxException, IOException {
        if(args.length < 2) {
            System.out.println("[command] id secret");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsAccessKeyId", args[0]);
        conf.set("fs.s3n.awsSecretAccessKey", args[1]);
        IxS3FileSystem fs = new IxS3FileSystem();
        fs.initialize(new URI("s3i://indix-tmp/dummy"), conf);

        FSDataOutputStream stream = fs.create(new Path("s3i://indix-tmp/dummy/"+ System.currentTimeMillis()));
        stream.write("hello".getBytes());
        stream.close();

        for (FileStatus fileStatus : fs.listStatus(new Path("s3i://indix-tmp/dummy"))) {
            System.out.println(fileStatus.getPath());
        }

        fs.close();
        System.exit(0);
    }
}
