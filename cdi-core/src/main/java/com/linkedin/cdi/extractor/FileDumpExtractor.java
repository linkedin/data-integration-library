// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.FileDumpExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.preprocessor.OutputStreamProcessor;
import com.linkedin.cdi.preprocessor.StreamProcessor;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.VariableUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * FileDumpExtractor takes an InputStream, applies proper preprocessors, and saves the InputStream
 * to a file.
 */
public class FileDumpExtractor extends MultistageExtractor<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileDumpExtractor.class);
  private final static int HADOOP_DEFAULT_FILE_LENGTH_LIMIT = 255;
  private FileDumpExtractorKeys fileDumpExtractorKeys = new FileDumpExtractorKeys();

  public FileDumpExtractorKeys getFileDumpExtractorKeys() {
    return fileDumpExtractorKeys;
  }

  public FileDumpExtractor(WorkUnitState state, JobKeys jobKeys) {
    super(state, jobKeys);
    super.initialize(fileDumpExtractorKeys);
    initialize(fileDumpExtractorKeys);
  }

  @Override
  protected void initialize(ExtractorKeys keys) {
    fileDumpExtractorKeys.logUsage(state);
    // initialize FileDumpExtractor keys
    // Extractors follow the pattern of initializing in constructor to avoid forgetting initialization
    // in sub-classes
    if (DATA_PUBLISHER_FINAL_DIR.isValidNonblank(state)) {
      fileDumpExtractorKeys.setFileDumpLocation(DATA_PUBLISHER_FINAL_DIR.get(state));
    } else {
      throw new RuntimeException("data publisher final dir is empty or null");
    }

    // file permission is required, but a default value is given in PropertyCollection
    fileDumpExtractorKeys.setFileWritePermissions(
        MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION.get(state));

    // work unit file name is based on a template that is defined by ms.extractor.target.file.name
    // and then substituted with activation parameters
    // TODO to allow substitution of variables defined in ms.parameters
    fileDumpExtractorKeys.setFileName(getFileName(state));

    fileDumpExtractorKeys.logDebugAll(state.getWorkunit());
  }

  /**
   * Utility function to do a double assignment
   * @param fileDumpExtractorKeys the extractor key
   */
  @VisibleForTesting
  protected void setFileDumpExtractorKeys(FileDumpExtractorKeys fileDumpExtractorKeys) {
    this.extractorKeys = fileDumpExtractorKeys;
    this.fileDumpExtractorKeys = fileDumpExtractorKeys;
  }

  /**
   * This method rely on the parent class to get a JsonArray formatted schema, and pass it out as
   * a string. Typically we expect the downstream is a CsvToJsonConverter.
   *
   * @return schema that is structured as a JsonArray but formatted as a String
   */
  @Override
  public String getSchema() {
    return super.getOrInferSchema().toString();
  }


  /**
   * TODO return 1 record of the file metadata like path, size, and timestamp, etc.
   * For dumping files on hdfs we don't need to return a specific record but just save file on hdfs and return null.
   */
  @Nullable
  @Override
  public String readRecord(String reuse) {
    workUnitStatus.setPageStart(fileDumpExtractorKeys.getCurrentFileNumber()
        * jobKeys.getPaginationInitValues().getOrDefault(ParameterTypes.PAGESIZE, 1L));
    workUnitStatus.setPageNumber(fileDumpExtractorKeys.getCurrentFileNumber() + 1);
    workUnitStatus.setPageSize(jobKeys.getPaginationInitValues().getOrDefault(ParameterTypes.PAGESIZE, 1L));

    if (processInputStream(this.fileDumpExtractorKeys.getCurrentFileNumber())
        && jobKeys.isPaginationEnabled()) {
      this.fileDumpExtractorKeys.incrCurrentFileNumber();
      return readRecord(reuse);
    }
    return null;
  }

  /**
   * This is the main method in this extractor, it extracts data from source and perform essential checks.
   *
   * @param starting the initial record count, indicating if it is the first of a series of requests
   * @return true if Successful
   */
  @Override
  protected boolean processInputStream(long starting) {
    if (!super.processInputStream(starting)) {
      return false;
    }

    if (StringUtils.isBlank(fileDumpExtractorKeys.getFileName())) {
      LOG.error("File name is empty so cannot dump onto the file system.");
      this.state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      return false;
    }

    if (workUnitStatus.getBuffer() == null) {
      LOG.info("Received a NULL InputStream, end the work unit");
      return false;
    }

    try {
      InputStream input = workUnitStatus.getBuffer();
      String fileName = fileDumpExtractorKeys.getFileDumpLocation() + "/"
          + fileDumpExtractorKeys.getFileName();
      if (jobKeys.isPaginationEnabled()) {
        fileName += "_";
        fileName += this.fileDumpExtractorKeys.getCurrentFileNumber();
      }
      writeToFileSystem(input, fileName);
    } catch (Exception e) {
      LOG.error("Error while extracting from source or writing to target", e);
      this.state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      return false;
    }
    return true;
  }

  /**
   * write an input stream at the dump location.
   */
  private void writeToFileSystem(InputStream is, String dumplocation) {
    Preconditions.checkNotNull(is, "InputStream");
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      FsPermission logPermission = new FsPermission(fileDumpExtractorKeys.getFileWritePermissions());

      // handle file name extensions
      String path = dumplocation;
      for (StreamProcessor<?> transformer : extractorKeys.getPreprocessors()) {
        if (transformer instanceof OutputStreamProcessor) {
          path = ((OutputStreamProcessor) transformer).convertFileName(path);
        }
      }

      // create output stream after renaming the file with proper extensions if needed
      // if there is a output preprocessor, like GPG encryptor, specified
      OutputStream os = FileSystem.create(fs, new Path(path), logPermission);
      for (StreamProcessor<?> transformer : extractorKeys.getPreprocessors()) {
        if (transformer instanceof OutputStreamProcessor) {
          os = ((OutputStreamProcessor) transformer).process(os);
        }
      }

      byte[] buffer = new byte[8192];
      long totalBytes = 0;
      int len = 0;
      while ((len = is.read(buffer)) != -1) {
        os.write(buffer, 0, len);
        totalBytes += len;
      }
      is.close();
      os.flush();
      os.close();
      LOG.info("FileDumpExtractor: written {} bytes to file {}", totalBytes, dumplocation);
    } catch (IOException e) {
      throw new RuntimeException("Unable to dump file at specified location from FileDumpExtractor", e);
    }
  }

  /**
   * TODO allow ms.extractor.target.file.name to use variables defined in ms.parameters
   * TODO encode or remove restricted characters from file name
   * Figure out what the file name should be based on the file name template and activation parameters
   * @param state work unit state contains key configuration
   * @return the file name
   */
  private String getFileName(WorkUnitState state) {
    String fileNameTemplate = MSTAGE_EXTRACTOR_TARGET_FILE_NAME.get(state);
    JsonObject activationParameters = extractorKeys.getActivationParameters();
    try {
      String filePath = VariableUtils.replaceWithTracking(fileNameTemplate, activationParameters).getKey();
      List<String> segments = Lists.newArrayList(filePath.split(Path.SEPARATOR));
      String fileName = segments.get(segments.size() - 1);
      if (fileName.length() > HADOOP_DEFAULT_FILE_LENGTH_LIMIT) {
        LOG.warn("File name is truncated to {} characters", HADOOP_DEFAULT_FILE_LENGTH_LIMIT);
        fileName = fileName.substring(0, HADOOP_DEFAULT_FILE_LENGTH_LIMIT - 1);
      }
      segments.remove(segments.size() - 1);
      segments.add(fileName);
      return Joiner.on(Path.SEPARATOR_CHAR).join(segments);
    } catch (Exception e) {
      LOG.error("Error resolving placeholders in {}", MSTAGE_EXTRACTOR_TARGET_FILE_NAME.toString());
      LOG.error("The value \"{}\" will be used as is", fileNameTemplate);
      return fileNameTemplate;
    }
  }

  /*
  TODO : Support reading file from hdfs again to apply transformation with GZIPInputStream
  private InputStream readFromFileSystem(String location) {
    InputStream in = null;
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      in = fs.open(new Path(location));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return in;
  } */
}
