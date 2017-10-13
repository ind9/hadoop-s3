package org.apache.hadoop.fs;

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
                "-mv",
                "s3i://indix-tmp/dummy",
                "s3i://indix-tmp/dummy2"
        });
    }
}
