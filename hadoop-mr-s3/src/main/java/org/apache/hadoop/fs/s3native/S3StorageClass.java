package org.apache.hadoop.fs.s3native;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

public enum S3StorageClass {
    REDUCED_REDUNDANCY("REDUCED_REDUNDANCY"),
    STANDARD("STANDARD"),
    GLACIER("GLACIER"),
    STANDARD_IA("STANDARD_IA");

    private String storageClass;

    S3StorageClass(String name) {
        this.storageClass = name;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public static S3StorageClass get(String storageClass) {
        if(StringUtils.isBlank(storageClass)) return STANDARD;
        else if (StringUtils.equalsIgnoreCase(storageClass, "standard")) return STANDARD;
        else if (StringUtils.equalsIgnoreCase(storageClass, "rrs")) return REDUCED_REDUNDANCY;
        else if (StringUtils.equalsIgnoreCase(storageClass, "glacier")) return GLACIER;
        // TODO - Can we directly write on Standard-IA storage on S3 or should it always be moved via LifeCycle?
        else if (StringUtils.equalsIgnoreCase(storageClass, "standard-ia")) return STANDARD_IA;
        else throw new RuntimeException("Invalid Storage class specified - " + storageClass);
    }
}
