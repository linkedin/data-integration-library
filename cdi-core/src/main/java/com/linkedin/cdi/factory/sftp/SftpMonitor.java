// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.SftpProgressMonitor;
import com.linkedin.cdi.factory.DefaultConnectionClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an SftpProgressMonitor to monitor the progress of file transfers using the ChannelSftp.GET
 * and ChannelSftp.PUT methods
 */
public class SftpMonitor implements SftpProgressMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultConnectionClientFactory.class);

  private int op;
  private String src;
  private String dest;
  private long totalCount;
  private long logFrequency;
  private long startTime;

  public int getOp() {
    return op;
  }

  public void setOp(int op) {
    this.op = op;
  }

  public String getSrc() {
    return src;
  }

  public void setSrc(String src) {
    this.src = src;
  }

  public String getDest() {
    return dest;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public void setTotalCount(long totalCount) {
    this.totalCount = totalCount;
  }

  public long getLogFrequency() {
    return logFrequency;
  }

  public void setLogFrequency(long logFrequency) {
    this.logFrequency = logFrequency;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public void init(int op, String src, String dest, long max) {
    this.op = op;
    this.src = src;
    this.dest = dest;
    this.startTime = System.currentTimeMillis();
    this.logFrequency = 0L;
    if (op == SftpProgressMonitor.GET) {
      LOG.info("DOWNLOAD operation has started with src: " + src + " dest: " + dest + " and file length: " + (max
          / 1000000L) + " mb");
    } else {
      LOG.info("UPLOAD operation has started with src: " + src + " dest: " + dest);
    }
  }

  @Override
  public boolean count(long count) {
    this.totalCount += count;

    if (this.logFrequency == 0L) {
      this.logFrequency = 1000L;
      if (op == SftpProgressMonitor.GET) {
        LOG.info("Transfer is in progress for file: " + this.src + ". Finished transferring " + this.totalCount + " bytes ");
      } else {
        LOG.info("Upload in progress for file " + this.dest + ". Finished uploading" + this.totalCount + " bytes");
      }
      long mb = this.totalCount / 1000000L;
      LOG.info("Transferred " + mb + " Mb. Speed " + getMbps() + " Mbps");
    }
    this.logFrequency--;
    return true;
  }

  @Override
  public void end() {
    long secs = (System.currentTimeMillis() - this.startTime) / 1000L;
    LOG.info("Transfer finished " + this.op + " src: " + this.src + " dest: " + this.dest + " in " + secs + " at "
        + getMbps());
  }

  private String getMbps() {
    long mb = this.totalCount / 1000000L;
    long secs = (System.currentTimeMillis() - this.startTime) / 1000L;
    double mbps = secs == 0L ? 0.0D : mb * 1.0D / secs;
    return String.format("%.2f", mbps);
  }
}
