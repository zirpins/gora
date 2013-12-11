package org.apache.gora.cassandra.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.mapreduce.GoraMapReduceUtils;
import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extended GoraInputFormat allowing to feed an arbitrary query set into a mapred job.
 *
 * The splitter part strictly creates one split per query.
 *
 * @author Christian Zirpins (c.zirpins@seeburger.de)
 */
public class GoraQueriesInputFormat<K, T extends PersistentBase> extends InputFormat<K, T> implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(GoraQueriesInputFormat.class);

  public static final String QUERIES_KEY = "gora.inputformat.queries";

  private static final boolean DEBUG = false;

  private static boolean reuseSerializationObjects;

  private Configuration conf;

  private Query<K, T>[] queries;

  /**
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit,
   *      org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  @SuppressWarnings("unchecked")
  public RecordReader<K, T> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    if (DEBUG)
      LOG.debug("Creating record reader from Split with query:" + ((GoraInputSplit) split).getQuery());
    PartitionQuery<K, T> partitionQuery = (PartitionQuery<K, T>) ((GoraInputSplit) split).getQuery();
    return new GoraRecordReader<K, T>(partitionQuery, context);
  }

  /**
   * This splitter generates splits for each query of the set. It does not consider partitionQueries
   * that are created by the datastore
   *
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (Query<K, T> baseQuery : queries) {
      PartitionQueryImpl<K, T> partQuery = new PartitionQueryImpl<K, T>(baseQuery, baseQuery.getStartKey(), baseQuery.getEndKey());
      GoraInputSplit split = new GoraInputSplit(context.getConfiguration(), partQuery);
      splits.add(split);
    }
    if (DEBUG)
      LOG.debug("Created " + splits.size() + " input splits");
    return splits;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      this.queries = getQueries(conf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * @param job
   * @param queries
   * @throws IOException
   */
  public static <K, T extends Persistent> void setQueries(Job job, Query<K, T>[] queries) throws IOException {
    ExtendedIOUtils.storeArrayToConf(queries, job.getConfiguration(), QUERIES_KEY);
  }

  /**
   * @param conf
   * @return
   * @throws IOException
   */
  public Query<K, T>[] getQueries(Configuration conf) throws IOException {
    return ExtendedIOUtils.loadArrayFromConf(conf, QUERIES_KEY);
  }

  /**
   * Sets the input parameters for the job
   *
   * @param job
   *          the job to set the properties for
   * @param querySet
   *          the query set to get the inputs from
   * @param reuseObjects
   *          whether to reuse objects in serialization
   * @throws IOException
   */
  public static <K1, V1 extends Persistent> void setInput(Job job, Query<K1, V1>[] queries, boolean reuseObjects) throws IOException {
    setInput(job, queries, queries[0].getDataStore(), reuseObjects);
  }

  /**
   * Sets the input parameters for the job
   *
   * @param job
   *          the job to set the properties for
   * @param querySet
   *          the query set to get the inputs from
   * @param dataStore
   *          the datastore as the input
   * @param reuseObjects
   *          whether to reuse objects in serialization
   * @throws IOException
   */
  public static <K1, V1 extends Persistent> void setInput(Job job, Query<K1, V1>[] queries, DataStore<K1, V1> dataStore, boolean reuseObjects)
      throws IOException {

    GoraQueriesInputFormat.reuseSerializationObjects = reuseObjects;

    Configuration conf = job.getConfiguration();

    GoraMapReduceUtils.setIOSerializations(conf, GoraQueriesInputFormat.reuseSerializationObjects);

    job.setInputFormatClass(GoraQueriesInputFormat.class);
    GoraQueriesInputFormat.setQueries(job, queries);

    if (DEBUG)
      LOG.debug("SetInput with " + queries.length + " queries.");
  }
}
