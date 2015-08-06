package org.apache.hadoop.fs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class IxS3Path {
    private static final String[] prefixes =    {"0","1","2","3","4","5","6","7","8","9",
                                                "a","b","c","d","e","f","g","h","i","j",
                                                "k","l","m","n","o","p","q","r","s","t",
                                                "u","v","w","x","y","z"};

    private static final Random ramdomGenerator = new Random();

    public static List<Path> allPaths(Path path) {
        List<Path> paths = new ArrayList<Path>();
        for (String prefix : prefixes) {
            paths.add(replaceSchemeAndPrefix(path, prefix));
        }
        return paths;
    }

    public static Path pickRandomPath(Path path) {
        return replaceSchemeAndPrefix(path, prefixes[Math.abs(ramdomGenerator.nextInt())% prefixes.length]);
    }

    private static Path replaceSchemeAndPrefix(Path path, String prefix) {
        URI uri = path.toUri();
        try {
            return new Path(new URI("s3n", uri.getAuthority(), "/" + prefix + uri.getPath(), uri.getQuery(), uri.getFragment()));
        }catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Path toIxS3Path(Path path) {
        URI uri = path.toUri();
        try {
            return new Path(new URI("s3i", uri.getAuthority(), uri.getPath().replaceAll("^/[^/]*", ""), uri.getQuery(), uri.getFragment()));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
