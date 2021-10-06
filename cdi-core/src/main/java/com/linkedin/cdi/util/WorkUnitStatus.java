// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;


/**
 * Define a structure for data interchange between a Source and a Extractor.
 *
 * @author chrli
 */
public class WorkUnitStatus {
  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(WorkUnitStatus.class);
  private long totalCount;
  private long setCount;
  private long pageNumber = 0;
  private long pageStart = 0;
  private long pageSize = 0;
  private InputStream buffer;

  private Map<String, String> messages;
  private String sessionKey;

  WorkUnitStatus(long totalCount, long setCount, long pageNumber, long pageStart, long pageSize, InputStream buffer,
      Map<String, String> messages, String sessionKey) {
    this.totalCount = totalCount;
    this.setCount = setCount;
    this.pageNumber = pageNumber;
    this.pageStart = pageStart;
    this.pageSize = pageSize;
    this.buffer = buffer;
    this.messages = messages == null ? new HashMap<>() : messages;
    this.sessionKey = sessionKey;
  }

  public static WorkUnitStatusBuilder builder() {
    return new WorkUnitStatusBuilder();
  }

  /**
   *  retrieve source schema if provided
   *
   * @return source schema if provided
   */
  public JsonArray getSchema() {
    if (messages != null && messages.containsKey("schema")) {
      try {
        return new Gson().fromJson(messages.get("schema"), JsonArray.class);
      } catch (Exception e) {
        LOG.warn("Error reading source schema", e);
      }
    }
    return new JsonArray();
  }

  public void logDebugAll() {
    LOG.debug("These are values in WorkUnitStatus");
    LOG.debug("Total count: {}", totalCount);
    LOG.debug("Chunk count: {}", setCount);
    LOG.debug("Pagination: {},{},{}", pageStart, pageSize, pageNumber);
    LOG.debug("Session Status: {}", sessionKey);
    LOG.debug("Messages: {}", messages.toString());
  }

  public long getTotalCount() {
    return this.totalCount;
  }

  public long getSetCount() {
    return this.setCount;
  }

  public long getPageNumber() {
    return this.pageNumber;
  }

  public long getPageStart() {
    return this.pageStart;
  }

  public long getPageSize() {
    return this.pageSize;
  }

  public InputStream getBuffer() {
    return this.buffer;
  }

  public Map<String, String> getMessages() {
    return messages;
  }

  public String getSessionKey() {
    return this.sessionKey == null ? StringUtils.EMPTY : sessionKey;
  }

  public WorkUnitStatus setTotalCount(long totalCount) {
    this.totalCount = totalCount;
    return this;
  }

  public WorkUnitStatus setSetCount(long setCount) {
    this.setCount = setCount;
    return this;
  }

  public WorkUnitStatus setPageNumber(long pageNumber) {
    this.pageNumber = pageNumber;
    return this;
  }

  public WorkUnitStatus setPageStart(long pageStart) {
    this.pageStart = pageStart;
    return this;
  }

  public WorkUnitStatus setPageSize(long pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public WorkUnitStatus setBuffer(InputStream buffer) {
    this.buffer = buffer;
    return this;
  }

  public WorkUnitStatus setMessages(Map<String, String> messages) {
    this.messages = messages == null ? new HashMap<>() : messages;
    return this;
  }

  public WorkUnitStatus setSessionKey(String sessionKey) {
    this.sessionKey = sessionKey;
    return this;
  }

  public String toString() {
    return "WorkUnitStatus(totalCount=" + this.getTotalCount() + ", setCount=" + this.getSetCount() + ", pageNumber="
        + this.getPageNumber() + ", pageStart=" + this.getPageStart() + ", pageSize=" + this.getPageSize() + ", buffer="
        + this.getBuffer() + ", messages=" + this.getMessages() + ", sessionKey=" + this.getSessionKey() + ")";
  }

  public WorkUnitStatusBuilder toBuilder() {
    return new WorkUnitStatusBuilder().totalCount(this.totalCount)
        .setCount(this.setCount)
        .pageNumber(this.pageNumber)
        .pageStart(this.pageStart)
        .pageSize(this.pageSize)
        .buffer(this.buffer)
        .messages(this.messages)
        .sessionKey(this.sessionKey);
  }

  public static class WorkUnitStatusBuilder {
    private long totalCount;
    private long setCount;
    private long pageNumber;
    private long pageStart;
    private long pageSize;
    private InputStream buffer;
    private Map<String, String> messages;
    private String sessionKey;

    WorkUnitStatusBuilder() {
    }

    public WorkUnitStatus.WorkUnitStatusBuilder totalCount(long totalCount) {
      this.totalCount = totalCount;
      return this;
    }

    public WorkUnitStatus.WorkUnitStatusBuilder setCount(long setCount) {
      this.setCount = setCount;
      return this;
    }

    public WorkUnitStatus.WorkUnitStatusBuilder pageNumber(long pageNumber) {
      this.pageNumber = pageNumber;
      return this;
    }

    public WorkUnitStatus.WorkUnitStatusBuilder pageStart(long pageStart) {
      this.pageStart = pageStart;
      return this;
    }

    public WorkUnitStatus.WorkUnitStatusBuilder pageSize(long pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public WorkUnitStatus.WorkUnitStatusBuilder buffer(InputStream buffer) {
      this.buffer = buffer;
      return this;
    }

    public WorkUnitStatus.WorkUnitStatusBuilder messages(Map<String, String> messages) {
      this.messages = messages == null ? new HashMap<>() : messages;
      return this;
    }

    public WorkUnitStatus.WorkUnitStatusBuilder sessionKey(String sessionKey) {
      this.sessionKey = sessionKey;
      return this;
    }

    public WorkUnitStatus build() {
      return new WorkUnitStatus(totalCount, setCount, pageNumber, pageStart, pageSize, buffer, messages, sessionKey);
    }

    public String toString() {
      return "WorkUnitStatus.WorkUnitStatusBuilder(totalCount=" + this.totalCount + ", setCount=" + this.setCount
          + ", pageNumber=" + this.pageNumber + ", pageStart=" + this.pageStart + ", pageSize=" + this.pageSize
          + ", buffer=" + this.buffer + ", messages=" + this.messages + ", sessionKey=" + this.sessionKey + ")";
    }
  }
}
