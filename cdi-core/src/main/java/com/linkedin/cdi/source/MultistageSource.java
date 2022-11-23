// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.cdi.event.EventHelper;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.factory.producer.EventReporter;
import com.linkedin.cdi.factory.producer.EventReporterFactory;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.EndecoUtils;
import com.linkedin.cdi.util.WatermarkDefinition;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;
import static com.linkedin.cdi.util.WatermarkDefinition.WatermarkTypes.*;


/**
 * This is the base Source class of multi-stage connectors.
 *
 * MultistageSource, like other Gobblin Source classes, is responsible
 * for planning. Specifically MultistageSource has following functions:
 *
 * 1. Generate work units when called by Gobblin Framework
 * 2. Instantiate Extractors
 *
 * Gobblin first instantiate a MultistageSource from one of its sub-classes,
 * then calls the getWorkUnits() method. The input to getWorkUnits() is SourceState.
 *
 * After getting a list of work units, Gobblin will instantiate again one
 * MultistageSource from one of its sub-classes for each of the work unit,
 * and then call the getExtractor() method on each instance. The input to
 * getExtractor() is WorkUnitState.
 *
 * @author chrli
 * @param <S> The schema class
 * @param <D> The data class
 */
@SuppressWarnings("unchecked")
public class MultistageSource<S, D> extends AbstractSource<S, D> {
  private static final Logger LOG = LoggerFactory.getLogger(MultistageSource.class);
  final static private Gson GSON = new Gson();
  final static private String PROPERTY_SEPARATOR = ".";
  final static private String DUMMY_DATETIME_WATERMARK_START = "2019-01-01";
  final static private String CURRENT_DATE_SYMBOL = "-";
  final static private String ACTIVATION_WATERMARK_NAME = "activation";
  // Avoid too many partition created from misconfiguration, Months * Days * Hours

  protected SourceState sourceState = null;
  protected EventReporter eventReporter;
  JobKeys jobKeys = new JobKeys();

  public SourceState getSourceState() {
    return sourceState;
  }

  public void setSourceState(SourceState sourceState) {
    this.sourceState = sourceState;
  }

  public JobKeys getJobKeys() {
    return jobKeys;
  }

  public void setJobKeys(JobKeys jobKeys) {
    this.jobKeys = jobKeys;
  }

  final private ConcurrentHashMap<MultistageExtractor<S, D>, WorkUnitState> extractorState =
      new ConcurrentHashMap<>();

  protected void initialize(State state) {
    jobKeys.initialize(state);
  }

  /**
   * getWorkUnits() is the first place to receive the Source State object, therefore
   * initialization of parameters cannot be complete in constructor.
   */
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    sourceState = state;
    initialize(state);
    eventReporter = MSTAGE_METRICS_ENABLED.get(state) ? EventReporterFactory.getEventReporter(state) : null;

    if (eventReporter != null) {
      // JobKeys initialization event
      eventReporter.send(EventHelper.createInitializationEvent(state, getClass().getName()));
    }

    if (!jobKeys.validate(state)) {
      throw new RuntimeException("Some parameters are invalid, job will do nothing until they are fixed.");
    }
    jobKeys.logUsage(state);

    // Parse watermark settings if defined
    List<WatermarkDefinition> definedWatermarks = Lists.newArrayList();
    for (JsonElement definitionJson : jobKeys.getWatermarkDefinition()) {
      definedWatermarks.add(new WatermarkDefinition(
          definitionJson.getAsJsonObject(), jobKeys.getIsPartialPartition(),
          jobKeys.getWorkUnitPartitionType()));
    }

    Map<String, JsonArray> secondaryInputs = MSTAGE_SECONDARY_INPUT.readAllContext(sourceState);
    JsonArray authentications = secondaryInputs.get(KEY_WORD_AUTHENTICATION);
    JsonArray activations = secondaryInputs.computeIfAbsent(KEY_WORD_ACTIVATION, x -> new JsonArray());
    JsonArray payloads = secondaryInputs.computeIfAbsent(KEY_WORD_PAYLOAD, x -> new JsonArray());

    // create a dummy activation if there is no activation secondary input nor defined unit watermark
    if (activations.size() == 0
        && definedWatermarks.stream().noneMatch(x -> x.getType().equals(UNIT))
        && payloads.size() != 0) {
      JsonObject simpleActivation = new JsonObject();
      activations.add(simpleActivation);
    }

    if (activations.size() > 0) {
      definedWatermarks.add(new WatermarkDefinition(ACTIVATION_WATERMARK_NAME, activations));
    }

    // Get previous high watermarks by each watermark partition or partition combinations
    // if there are multiple partitioned watermarks, such as one partitioned datetime watermark
    // and one partitioned activation (unit) watermark.
    // Ignore previous high watermark in back fill mode, and avoid doing a full extract
    Map<String, Long> previousHighWatermarks = MSTAGE_BACKFILL.get(state)
        ? new HashMap<>() : getPreviousHighWatermarks();
    state.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, MSTAGE_BACKFILL.get(state)
        ? Boolean.FALSE : checkFullExtractState(state, previousHighWatermarks));

    // generated work units based on watermarks defined and previous high watermarks
    List<WorkUnit> wuList = generateWorkUnits(definedWatermarks, previousHighWatermarks);

    // abort (fail) the job when the number of work units is below require threshold
    if (wuList.size() < jobKeys.getMinWorkUnits()) {
      throw new RuntimeException(String.format(EXCEPTION_WORK_UNIT_MINIMUM,
          jobKeys.getMinWorkUnits(),
          jobKeys.getMinWorkUnits()));
    }

    for (WorkUnit wu : wuList) {
      if (authentications != null && authentications.size() == 1) {
        wu.setProp(MSTAGE_ACTIVATION_PROPERTY.toString(),
            getUpdatedWorkUnitActivation(wu, authentications.get(0).getAsJsonObject()));
      }
      // unlike activation secondary inputs, payloads will be processed in each work unit
      // and payloads will not be loaded until the Connection executes the command
      wu.setProp(MSTAGE_PAYLOAD_PROPERTY.toString(), payloads);
    }

    if (eventReporter != null) {
      // Send work unit creation event
      eventReporter.send(EventHelper.createWorkunitCreationEvent(state,wuList,getClass().getName()));
    }
    return wuList;
  }

  /**
   * Default multi-stage source behavior, each protocol shall override this with more concrete function
   * @param state WorkUnitState passed in from Gobblin framework
   * @return an MultistageExtractor instance
   */
  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) {
    try {
      ClassLoader loader = this.getClass().getClassLoader();
      Class<?> extractorClass = loader.loadClass(MSTAGE_EXTRACTOR_CLASS.get(state));
      Constructor<MultistageExtractor<?, ?>> constructor = (Constructor<MultistageExtractor<?, ?>>)
          extractorClass.getConstructor(WorkUnitState.class, JobKeys.class);
      MultistageExtractor<S, D> extractor = (MultistageExtractor<S, D>) constructor.newInstance(state, this.jobKeys);
      extractorState.put(extractor, state);
      extractor.setConnection(null);
      return extractor;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * provide a default implementation
   * @param state Source State
   */
  @Override
  public void shutdown(SourceState state) {
    LOG.info("MultistageSource Shutdown() called, instructing extractors to close connections");
    for (MultistageExtractor<S, D> extractor: extractorState.keySet()) {
      extractor.closeConnection();
    }
    if (eventReporter != null) {
      eventReporter.close();
    }

  }

  List<WorkUnit> generateWorkUnits(List<WatermarkDefinition> definitions, Map<String, Long> previousHighWatermarks) {
    Assert.assertNotNull(sourceState);
    WatermarkDefinition datetimeWatermark = null;
    WatermarkDefinition unitWatermark = null;

    for (WatermarkDefinition wmd: definitions) {
      if (wmd.getType() == WatermarkDefinition.WatermarkTypes.DATETIME) {
        if (datetimeWatermark != null) {
          throw new RuntimeException("1 and only datetime type watermark is allowed.");
        }
        datetimeWatermark = wmd;
      }
      if (wmd.getType() == UNIT) {
        if (unitWatermark != null) {
          throw new RuntimeException("1 and only unit type watermark is allowed"
              + ", including the unit watermark generated from secondary input.");
        }
        unitWatermark = wmd;
      }
    }
    // Set default unit watermark
    if (unitWatermark == null) {
      // abort (fail) the job when at least some work units are expected
      if (jobKeys.getMinWorkUnits() > 0) {
        throw new RuntimeException(String.format(EXCEPTION_WORK_UNIT_MINIMUM,
            jobKeys.getMinWorkUnits(),
            jobKeys.getMinWorkUnits()));
      }

      JsonArray unitArray = new JsonArray();
      unitArray.add(new JsonObject());
      unitWatermark = new WatermarkDefinition("unit", unitArray);
    }
    // Set default datetime watermark
    if (datetimeWatermark == null) {
      datetimeWatermark = new WatermarkDefinition("datetime", DUMMY_DATETIME_WATERMARK_START, CURRENT_DATE_SYMBOL);
    }

    List<WorkUnit> workUnits = new ArrayList<>();
    Extract extract = createExtractObject(checkFullExtractState(sourceState, previousHighWatermarks));
    List<ImmutablePair<Long, Long>> datetimePartitions = getDatetimePartitions(datetimeWatermark.getRangeInDateTime());
    List<String> unitPartitions = unitWatermark.getUnits();

    JsonArray watermarkGroups = new JsonArray();
    String datetimeWatermarkName = datetimeWatermark.getLongName();
    String unitWatermarkName = unitWatermark.getLongName();
    watermarkGroups.add(datetimeWatermarkName);
    watermarkGroups.add(unitWatermarkName);

    // only create work unit when the high range is greater than cutoff time
    // cutoff time is moved backward by GRACE_PERIOD
    // cutoff time is moved forward by ABSTINENT_PERIOD
    Long cutoffTime = previousHighWatermarks.size() == 0 ? -1 : Collections.max(previousHighWatermarks.values())
        - MSTAGE_GRACE_PERIOD_DAYS.getMillis(sourceState);
    LOG.debug("Overall cutoff time: {}", cutoffTime);

    for (ImmutablePair<Long, Long> dtPartition : datetimePartitions) {
      LOG.debug("dtPartition: {}", dtPartition);
      for (String unitPartition: unitPartitions) {

        // adding the date time partition and unit partition combination to work units until
        // it reaches ms.work.unit.parallelism.max. a combination is not added if its prior
        // watermark doesn't require a rerun.
        // a work unit signature is a date time partition and unit partition combination.
        if (MSTAGE_WORK_UNIT_PARALLELISM_MAX.isValidNonblank(sourceState)
            && workUnits.size() >= (Integer) MSTAGE_WORK_UNIT_PARALLELISM_MAX.get(sourceState)) {
          break;
        }

        // each work unit has a watermark since we use dataset.urn to track state
        // and the work unit can be uniquely identified by its signature
        String wuSignature = getWorkUnitSignature(
            datetimeWatermarkName, dtPartition.getLeft(),
            unitWatermarkName, unitPartition);
        LOG.debug("Checking work unit: {}", wuSignature);

        // if a work unit exists in state store, manage its watermark independently
        long unitCutoffTime = -1L;
        if (previousHighWatermarks.containsKey(wuSignature)) {
          unitCutoffTime = previousHighWatermarks.get(wuSignature)
              - MSTAGE_GRACE_PERIOD_DAYS.getMillis(sourceState)
              + MSTAGE_ABSTINENT_PERIOD_DAYS.getMillis(sourceState);
        }
        LOG.debug(String.format("previousHighWatermarks.get(wuSignature): %s, unitCutoffTime: %s",
            previousHighWatermarks.get(wuSignature), unitCutoffTime));

        // for a dated work unit partition, we only need to redo it when its previous
        // execution was not successful
        // for recent work unit partitions, we might need to re-extract based on
        // grace period logic, which is controlled by cut off time
        if (unitCutoffTime == -1L
            || dtPartition.getRight() >= Longs.max(unitCutoffTime, cutoffTime)) {
          // prune the date range only if the unit is not in first execution
          // note the nominal date range low boundary had been saved in signature
          ImmutablePair<Long, Long> dtPartitionModified = unitCutoffTime == -1L
              ? dtPartition : previousHighWatermarks.get(wuSignature).equals(dtPartition.left)
              ? dtPartition : new ImmutablePair<>(Long.max(unitCutoffTime, dtPartition.left), dtPartition.right);
          LOG.info(String.format(MSG_WORK_UNIT_INFO, wuSignature, dtPartitionModified));
          if (jobKeys.shouldCleanseNoRangeWorkUnit()
              && (long) dtPartitionModified.left == dtPartitionModified.right) {
            LOG.info(String.format("Skipping no range work units with low watermark: %s, high watermark: %s",
                dtPartitionModified.left, dtPartitionModified.right));
            continue;
          }
          WorkUnit workUnit = WorkUnit.create(extract,
              new WatermarkInterval(
                  new LongWatermark(dtPartitionModified.getLeft()),
                  new LongWatermark(dtPartitionModified.getRight())));

          // save work unit signature for identification
          // because each dataset URN key will have a state file on Hadoop, it cannot contain path separator
          workUnit.setProp(MSTAGE_WATERMARK_GROUPS.toString(),
              watermarkGroups.toString());
          workUnit.setProp(DATASET_URN.toString(), EndecoUtils.getHadoopFsEncoded(wuSignature));

          // save the lower number of datetime watermark partition and the unit watermark partition
          workUnit.setProp(datetimeWatermarkName, dtPartition.getLeft());
          workUnit.setProp(unitWatermarkName, unitPartition);

          workUnit.setProp(MSTAGE_ACTIVATION_PROPERTY.toString(), unitPartition);
          workUnit.setProp(MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME.toString(),
              DateTime.now().getMillis()
                  + workUnits.size() * MSTAGE_WORK_UNIT_PACING_SECONDS.getMillis(sourceState));

          if (!MSTAGE_OUTPUT_SCHEMA.isValidNonblank(sourceState)
            && this.jobKeys.hasOutputSchema()) {
            // populate the output schema read from URN reader to sub tasks
            // so that the URN reader will not be called again
            LOG.info("Populating output schema to work units:");
            LOG.info("Output schema: {}", this.jobKeys.getOutputSchema().toString());
            workUnit.setProp(MSTAGE_OUTPUT_SCHEMA.getConfig(),
                this.jobKeys.getOutputSchema().toString());

            // populate the target schema read from URN reader to sub tasks
            // so that the URN reader will not be called again
            LOG.info("Populating target schema to work units:");
            LOG.info("Target schema: {}", jobKeys.getTargetSchema().toString());
            workUnit.setProp(MSTAGE_TARGET_SCHEMA.getConfig(),
                jobKeys.getTargetSchema().toString());
          }
          workUnits.add(workUnit);
        }
      }
    }
    return workUnits;
  }

  /**
   * breaks a date time range to smaller partitions per WORK_UNIT_PARTITION property setting
   * if too many partitions created, truncate to the maximum partitions allowed
   *  @param datetimeRange a range of date time values
   * @return a list of data time ranges in milliseconds
   */
  private List<ImmutablePair<Long, Long>> getDatetimePartitions(ImmutablePair<DateTime, DateTime> datetimeRange) {
    List<ImmutablePair<Long, Long>> partitions = Lists.newArrayList();
    if (jobKeys.getWorkUnitPartitionType() != null) {
      partitions = jobKeys.getWorkUnitPartitionType().getRanges(datetimeRange,
          MSTAGE_WORK_UNIT_PARTIAL_PARTITION.get(sourceState));
    } else {
      partitions.add(new ImmutablePair<>(datetimeRange.getLeft().getMillis(), datetimeRange.getRight().getMillis()));
    }
    return partitions;
  }

  /**
   * Get all previous highest high watermarks, by dataset URN. If a dataset URN
   * had multiple work units, the highest high watermark is retrieved for that
   * dataset URN.
   *
   * @return the previous highest high watermarks by dataset URN
   */
  private Map<String, Long> getPreviousHighWatermarks() {
    Map<String, Long> watermarks = new HashMap<>();
    Map<String, Iterable<WorkUnitState>> wuStates = sourceState.getPreviousWorkUnitStatesByDatasetUrns();
    for (Map.Entry<String, Iterable<WorkUnitState>> entry: wuStates.entrySet()) {
      Long highWatermark = Collections.max(Lists.newArrayList(entry.getValue().iterator()).stream()
          .map(s -> s.getActualHighWatermark(LongWatermark.class).getValue())
          .collect(Collectors.toList()));

      // Unit watermarks might contain encoded file separator,
      // in such case, we will decode the watermark name so that it can be compared with
      // work unit signatures
      LOG.debug("Dataset Signature: {}, High Watermark: {}", EndecoUtils.getHadoopFsDecoded(entry.getKey()), highWatermark);
      watermarks.put(EndecoUtils.getHadoopFsDecoded(entry.getKey()), highWatermark);
    }
    return ImmutableMap.copyOf(watermarks);
  }

  Extract createExtractObject(final boolean isFull) {
    Extract extract = createExtract(
        Extract.TableType.valueOf(EXTRACT_TABLE_TYPE.get(sourceState)),
        EXTRACT_NAMESPACE.get(sourceState),
        EXTRACT_TABLE_NAME.get(sourceState));
    extract.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, isFull);
    return extract;
  }

  private String getWorkUnitSignature(
      String datetimeWatermarkName,
      Long datetimePartition,
      String unitWatermarkName,
      String unitPartition) {
    List<String> list = Lists.newArrayList(datetimeWatermarkName + PROPERTY_SEPARATOR + datetimePartition,
        unitWatermarkName + PROPERTY_SEPARATOR  + unitPartition);
    return list.toString();
  }

  /**
   * retrieve the authentication data from secondary input
   * TODO this is not being used in handling HTTP 403 error
   * @return the authentication JsonObject
   */
  protected JsonObject readSecondaryAuthentication(State state) throws InterruptedException {
    Map<String, JsonArray> secondaryInputs = MSTAGE_SECONDARY_INPUT.readAuthenticationToken(state);
    return secondaryInputs.get(KEY_WORD_AUTHENTICATION).get(0).getAsJsonObject();
  }

  /**
   * This updates the activation properties of the work unit if a new authentication token
   * become available
   * @param wu the work unit configuration
   * @param authentication the authentication token from, usually, the secondary input
   * @return the updated work unit configuration
   */
  protected String getUpdatedWorkUnitActivation(WorkUnit wu, JsonObject authentication) {
    LOG.debug("Activation property (origin): {}", wu.getProp(MSTAGE_ACTIVATION_PROPERTY.toString(), ""));
    if (!wu.getProp(MSTAGE_ACTIVATION_PROPERTY.toString(), StringUtils.EMPTY).isEmpty()) {
      JsonObject existing = GSON.fromJson(wu.getProp(MSTAGE_ACTIVATION_PROPERTY.toString()), JsonObject.class);
      for (Map.Entry<String, JsonElement> entry: authentication.entrySet()) {
        existing.remove(entry.getKey());
        existing.addProperty(entry.getKey(), entry.getValue().getAsString());
      }
      LOG.debug("Activation property (modified): {}", existing.toString());
      return existing.toString();
    }
    LOG.debug("Activation property (new): {}", authentication.toString());
    return authentication.toString();
  }

  /**
   * Check if a full extract is needed
   * @param state source state
   * @param previousHighWatermarks existing high watermarks
   * @return true if all conditions met for a full extract, otherwise false
   */
  private boolean checkFullExtractState(final State state, final Map<String, Long> previousHighWatermarks) {
    if (EXTRACT_TABLE_TYPE.get(state).toString()
        .equalsIgnoreCase(KEY_WORD_SNAPSHOT_ONLY)) {
      return true;
    }

    if (MSTAGE_ENABLE_DYNAMIC_FULL_LOAD.get(state)) {
      if (previousHighWatermarks.isEmpty()) {
        return true;
      }
    }

    return state.getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY, false);
  }
}
