/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3native;

import static org.apache.hadoop.fs.s3native.NativeS3FileSystem.PATH_DELIMITER;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.HttpException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.MultipartPart;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Jets3tNFSStoreWithStorageClassSupport extends Jets3tNativeFileSystemStore {

  private S3Service s3Service;
  private S3Bucket bucket;
  private S3StorageClass storageClass;

  private long multipartBlockSize;
  private boolean multipartEnabled;
  private long multipartCopyBlockSize;
  static final long MAX_PART_SIZE = (long)5 * 1024 * 1024 * 1024;

  private String serverSideEncryptionAlgorithm;

  public static final Logger LOG =
          LoggerFactory.getLogger(Jets3tNativeFileSystemStore.class);

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    S3Credentials s3Credentials = new S3Credentials();
    s3Credentials.initialize(uri, conf);
    try {
      AWSCredentials awsCredentials =
              new AWSCredentials(s3Credentials.getAccessKey(),
                      s3Credentials.getSecretAccessKey());
      this.s3Service = new RestS3Service(awsCredentials);
    } catch (S3ServiceException e) {
      handleException(e);
    }
    multipartEnabled =
            conf.getBoolean("fs.s3n.multipart.uploads.enabled", false);
    multipartBlockSize = Math.min(
            conf.getLong("fs.s3n.multipart.uploads.block.size", 64 * 1024 * 1024),
            MAX_PART_SIZE);
    multipartCopyBlockSize = Math.min(
            conf.getLong("fs.s3n.multipart.copy.block.size", MAX_PART_SIZE),
            MAX_PART_SIZE);
    serverSideEncryptionAlgorithm = conf.get("fs.s3n.server-side-encryption-algorithm");
    // Acceptable values are - standard, glacier, rrs, standard-ia
    storageClass = S3StorageClass.get(conf.get("fs.s3n.storage.class", "standard"));

    bucket = new S3Bucket(uri.getHost());
  }

  @Override
  public void storeFile(String key, File file, byte[] md5Hash)
          throws IOException {

    if (multipartEnabled && file.length() >= multipartBlockSize) {
      storeLargeFile(key, file, md5Hash);
      return;
    }

    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      S3Object object = new S3Object(key);
      object.setStorageClass(storageClass.getStorageClass());
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(file.length());
      object.setServerSideEncryptionAlgorithm(serverSideEncryptionAlgorithm);
      if (md5Hash != null) {
        object.setMd5Hash(md5Hash);
      }
      s3Service.putObject(bucket, object);
    } catch (ServiceException e) {
      handleException(e, key);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public void storeLargeFile(String key, File file, byte[] md5Hash)
          throws IOException {
    S3Object object = new S3Object(key);
    object.setStorageClass(storageClass.getStorageClass());
    object.setDataInputFile(file);
    object.setContentType("binary/octet-stream");
    object.setContentLength(file.length());
    object.setServerSideEncryptionAlgorithm(serverSideEncryptionAlgorithm);
    if (md5Hash != null) {
      object.setMd5Hash(md5Hash);
    }

    List<StorageObject> objectsToUploadAsMultipart =
            new ArrayList<StorageObject>();
    objectsToUploadAsMultipart.add(object);
    MultipartUtils mpUtils = new MultipartUtils(multipartBlockSize);

    try {
      mpUtils.uploadObjects(bucket.getName(), s3Service,
              objectsToUploadAsMultipart, null);
    } catch (Exception e) {
      handleException(e, key);
    }
  }

  @Override
  public void storeEmptyFile(String key) throws IOException {
    try {
      S3Object object = new S3Object(key);
      object.setStorageClass(storageClass.getStorageClass());
      object.setDataInputStream(new ByteArrayInputStream(new byte[0]));
      object.setContentType("binary/octet-stream");
      object.setContentLength(0);
      object.setServerSideEncryptionAlgorithm(serverSideEncryptionAlgorithm);
      s3Service.putObject(bucket, object);
    } catch (ServiceException e) {
      handleException(e, key);
    }
  }

  public void copyLargeFile(S3Object srcObject, String dstKey) throws IOException {
    try {
      long partCount = srcObject.getContentLength() / multipartCopyBlockSize +
              (srcObject.getContentLength() % multipartCopyBlockSize > 0 ? 1 : 0);

      MultipartUpload multipartUpload = s3Service.multipartStartUpload
              (bucket.getName(), dstKey, srcObject.getMetadataMap(), null, storageClass.getStorageClass());

      List<MultipartPart> listedParts = new ArrayList<MultipartPart>();
      for (int i = 0; i < partCount; i++) {
        long byteRangeStart = i * multipartCopyBlockSize;
        long byteLength;
        if (i < partCount - 1) {
          byteLength = multipartCopyBlockSize;
        } else {
          byteLength = srcObject.getContentLength() % multipartCopyBlockSize;
          if (byteLength == 0) {
            byteLength = multipartCopyBlockSize;
          }
        }

        MultipartPart copiedPart = s3Service.multipartUploadPartCopy
                (multipartUpload, i + 1, bucket.getName(), srcObject.getKey(),
                        null, null, null, null, byteRangeStart,
                        byteRangeStart + byteLength - 1, null);
        listedParts.add(copiedPart);
      }

      Collections.reverse(listedParts);
      s3Service.multipartCompleteUpload(multipartUpload, listedParts);
    } catch (ServiceException e) {
      handleException(e, srcObject.getKey());
    }
  }

  @Override
  public void dump() throws IOException {
    StringBuilder sb = new StringBuilder("S3 Native Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      S3Object[] objects = s3Service.listObjects(bucket.getName());
      for (S3Object object : objects) {
        sb.append(object.getKey()).append("\n");
      }
    } catch (S3ServiceException e) {
      handleException(e);
    }
    System.out.println(sb);
  }

  /**
   * Handle any service exception by translating it into an IOException
   * @param e exception
   * @throws IOException exception -always
   */
  private void handleException(Exception e) throws IOException {
    throw processException(e, e, "");
  }
  /**
   * Handle any service exception by translating it into an IOException
   * @param e exception
   * @param key key sought from object store

   * @throws IOException exception -always
   */
  private void handleException(Exception e, String key) throws IOException {
    throw processException(e, e, key);
  }

  /**
   * Handle any service exception by translating it into an IOException
   * @param thrown exception
   * @param original original exception -thrown if no other translation could
   * be made
   * @param key key sought from object store or "" for undefined
   * @return an exception to throw. If isProcessingCause==true this may be null.
   */
  private IOException processException(Throwable thrown, Throwable original,
                                       String key) {
    IOException result;
    if (thrown.getCause() != null) {
      // recurse down
      result = processException(thrown.getCause(), original, key);
    } else if (thrown instanceof HttpException) {
      // nested HttpException - examine error code and react
      HttpException httpException = (HttpException) thrown;
      String responseMessage = httpException.getResponseMessage();
      int responseCode = httpException.getResponseCode();
      String bucketName = "s3n://" + bucket.getName();
      String text = String.format("%s : %03d : %s",
              bucketName,
              responseCode,
              responseMessage);
      String filename = !key.isEmpty() ? (bucketName + "/" + key) : text;
      IOException ioe;
      switch (responseCode) {
        case 404:
          result = new FileNotFoundException(filename);
          break;
        case 416: // invalid range
          result = new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF
                  +": " + filename);
          break;
        case 403: //forbidden
          result = new AccessControlException("Permission denied"
                  +": " + filename);
          break;
        default:
          result = new IOException(text);
      }
      result.initCause(thrown);
    } else if (thrown instanceof S3ServiceException) {
      S3ServiceException se = (S3ServiceException) thrown;
      LOG.debug(
              "S3ServiceException: {}: {} : {}",
              se.getS3ErrorCode(), se.getS3ErrorMessage(), se, se);
      if ("InvalidRange".equals(se.getS3ErrorCode())) {
        result = new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
      } else {
        result = new S3Exception(se);
      }
    } else if (thrown instanceof ServiceException) {
      ServiceException se = (ServiceException) thrown;
      LOG.debug("S3ServiceException: {}: {} : {}",
              se.getErrorCode(), se.toString(), se, se);
      result = new S3Exception(se);
    } else if (thrown instanceof IOException) {
      result = (IOException) thrown;
    } else {
      // here there is no exception derived yet.
      // this means no inner cause, and no translation made yet.
      // convert the original to an IOException -rather than just the
      // exception at the base of the tree
      result = new S3Exception(original);
    }

    return result;
  }
}
