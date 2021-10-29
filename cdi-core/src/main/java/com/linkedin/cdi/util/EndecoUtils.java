// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * a collection of encoding and decoding functions
 */
public class EndecoUtils {
  private static final Logger LOG = LoggerFactory.getLogger(EndecoUtils.class);
  private EndecoUtils() {
    // hide constructor
  }

  /**
   * Decode an encoded URL string, complete or partial
   * @param encoded encoded URL string
   * @return decoded URL string
   */
  static public String decode(String encoded) {
    return decode(encoded, StandardCharsets.UTF_8.toString());
  }

  static public String decode(String encoded, String enc) {
    try {
      return URLDecoder.decode(encoded, enc);
    } catch (Exception e) {
      LOG.error("URL decoding error: " + e);
      return encoded;
    }
  }

  /**
   * Encode a URL string, complete or partial
   * @param plainUrl unencoded URL string
   * @return encoded URL string
   */
  static public String getEncodedUtf8(String plainUrl) {
    return getEncodedUtf8(plainUrl, StandardCharsets.UTF_8.toString());
  }

  static public String getEncodedUtf8(String plainUrl, String enc) {
    try {
      return URLEncoder.encode(plainUrl, enc);
    } catch (Exception e) {
      LOG.error("URL encoding error: " + e);
      return plainUrl;
    }
  }

  /**
   * Encode a Hadoop file name to encode path separator so that the file name has no '/'
   * @param fileName unencoded file name string
   * @return encoded path string
   */
  static public String getHadoopFsEncoded(String fileName) {
    return getHadoopFsEncoded(fileName, StandardCharsets.UTF_8.toString());
  }

  static public String getHadoopFsEncoded(String fileName, String enc) {
    try {
      String encodedSeparator = URLEncoder.encode(Path.SEPARATOR, enc);
      // we don't encode the whole string intentionally so that the state file name is more readable
      return fileName.replace(Path.SEPARATOR, encodedSeparator);
    } catch (Exception e) {
      LOG.error("Hadoop FS encoding error: " + e);
      return fileName;
    }
  }

  /**
   * Decode an encoded Hadoop file name to restore path separator
   * @param encodedFileName encoded file name string
   * @return encoded path string
   */
  static public String getHadoopFsDecoded(String encodedFileName) {
    return getHadoopFsDecoded(encodedFileName, StandardCharsets.UTF_8.toString());
  }

  static public String getHadoopFsDecoded(String encodedFileName, String enc) {
    try {
      String encodedSeparator = URLEncoder.encode(Path.SEPARATOR, enc);
      return encodedFileName.replace(encodedSeparator, Path.SEPARATOR);
    } catch (Exception e) {
      LOG.error("Hadoop FS decoding error: " + e);
      return encodedFileName;
    }
  }
}
