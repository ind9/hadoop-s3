package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class IxS3IntegrationTest {
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("[command] id secret");
            System.exit(1);
        }
        FsShell.main(new String[]{
                "-Dfs.s3n.awsAccessKeyId="+ args[0],
                "-Dfs.s3n.awsSecretAccessKey="+ args[1],
                "-Dfs.s3i.impl="+IxS3FileSystem.class.getName(),
                "-ls",
                "s3i://indix-tmp/dummy/"
        });
    }
}
