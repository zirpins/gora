package org.apache.gora.cassandra.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.mapreduce.GoraMapReduceUtils;
import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.FileSplitPartitionQuery;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.FileBackedDataStore;
import org.apache.gora.util.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Extended GoraInputFormat allowing to feed an arbitrary query set into a mapred job.
 *
 * The splitter part strictly creates one split per query.
 *
 * @author Christian Zirpins (c.zirpins@seeburger.de)
 */
public class GoraQuerySetInputFormat<K, T extends PersistentBase> extends InputFormat<K, T> implements Configurable {

  public static final String QUERYSET_KEY = "gora.inputformat.queryset";

  private Configuration conf;

  private Set<Query<K, T>> querySet;

  @SuppressWarnings({ "rawtypes" })
  private void setInputPath(PartitionQuery<K, T> partitionQuery, TaskAttemptContext context) throws IOException {
    // if the data store is file based
    if (partitionQuery instanceof FileSplitPartitionQuery) {
      FileSplit split = ((FileSplitPartitionQuery<K, T>) partitionQuery).getSplit();
      // set the input path to FileSplit's path.
      ((FileBackedDataStore) partitionQuery.getDataStore()).setInputPath(split.getPath().toString());
    }
  }

  /**
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit,
   *      org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  @SuppressWarnings("unchecked")
  public RecordReader<K, T> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    PartitionQuery<K, T> partitionQuery = (PartitionQuery<K, T>) ((GoraInputSplit) split).getQuery();
    setInputPath(partitionQuery, context);
    return new GoraRecordReader<K, T>(partitionQuery, context);
  }

  /**
   * This splitter generates splits for each query of the set. It does not consider
   * partitionQueries.
   *
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (Query<K, T> baseQuery : querySet) {
      PartitionQuery<K, T> partQuery = new PartitionQueryImpl<K, T>(baseQuery);
      splits.add(new GoraInputSplit(context.getConfiguration(), partQuery));
    }
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
      this.querySet = getQuerySet(conf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * @param job
   * @param queries
   * @throws IOException
   */
  public static <K, T extends Persistent> void setQuerySet(Job job, Set<Query<K, T>> queries) throws IOException {
    IOUtils.storeToConf(queries, job.getConfiguration(), QUERYSET_KEY);
  }

  /**
   * @param conf
   * @return
   * @throws IOException
   */
  public Set<Query<K, T>> getQuerySet(Configuration conf) throws IOException {
    return IOUtils.loadFromConf(conf, QUERYSET_KEY);
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
  public static <K1, V1 extends Persistent> void setInput(Job job, Set<Query<K1, V1>> querySet, boolean reuseObjects) throws IOException {
    setInput(job, querySet, querySet.iterator().next().getDataStore(), reuseObjects);
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
  public static <K1, V1 extends Persistent> void setInput(Job job, Set<Query<K1, V1>> querySet, DataStore<K1, V1> dataStore, boolean reuseObjects)
      throws IOException {

    Configuration conf = job.getConfiguration();

    GoraMapReduceUtils.setIOSerializations(conf, reuseObjects);

    job.setInputFormatClass(GoraInputFormat.class);
    GoraQuerySetInputFormat.setQuerySet(job, querySet);
  }
}
