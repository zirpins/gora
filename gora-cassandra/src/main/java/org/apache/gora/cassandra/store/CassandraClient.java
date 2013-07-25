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

package org.apache.gora.cassandra.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.gora.cassandra.query.CassandraQuery;
import org.apache.gora.cassandra.serializers.GoraSerializerTypeInferer;
import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.persistency.State;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClient<PK, T extends PersistentBase> {
  public static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);

  private static final String FIELD_SCAN_COLUMN_RANGE_DELIMITER_START = "!";
  private static final String FIELD_SCAN_COLUMN_RANGE_DELIMITER_END = "~";

  // define types of queries possible
  private enum CassandraQueryType {
    SINGLE, ROWSCAN, COLUMNSCAN, MULTISCAN, ROWSCAN_PRIMITIVE, SINGLE_PRIMITIVE;
  }

  // hector cluster representation
  private Cluster cluster;

  // hector keyspace representation (long lived)
  private Keyspace keyspace;

  // mutator
  private Mutator<DynamicComposite> mutator;

  // general mapping registry
  private CassandraMapping cassandraMapping = null;

  // key mapping functions
  private CassandraKeyMapper<PK, T> keyMapper;

  // primary key class (not to confuse with Cassandra row key)
  private Class<PK> primaryKeyClass;

  // persistent class
  private Class<T> persistentClass;

  public void initialize(Class<PK> keyClass, Class<T> persistentClass) throws Exception {
    this.setPrimaryKeyClass(keyClass);
    this.setPersistentClass(persistentClass);

    // get cassandra mapping with persistent class
    this.cassandraMapping = CassandraMappingManager.getManager().get(persistentClass);

    // init hector represetation of cassandra cluster
    this.cluster = HFactory.getOrCreateCluster(this.cassandraMapping.getClusterName(), new CassandraHostConfigurator(this.cassandraMapping.getHostName()));

    // add keyspace to cluster
    checkKeyspace();

    // Set a client-side customized default Consistency Level for all column families
    ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
    Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
    for (String familyName : this.cassandraMapping.getFamilies())
      clmap.put(familyName, HConsistencyLevel.ONE);
    configurableConsistencyLevel.setReadCfConsistencyLevels(clmap);
    configurableConsistencyLevel.setWriteCfConsistencyLevels(clmap);

    // initialize long-lived hector keyspace representation on client side
    keyspace = HFactory.createKeyspace(this.getKeyspaceName(), this.cluster, configurableConsistencyLevel);

    // initialize key mapper
    keyMapper = new CassandraKeyMapper<PK, T>(primaryKeyClass, cassandraMapping);

    // initialize mutator
    mutator = HFactory.createMutator(this.keyspace, new DynamicCompositeSerializer());
  }

  /**
   * Check if keyspace already exists.
   */
  public boolean keyspaceExists() {
    KeyspaceDefinition keyspaceDefinition = this.cluster.describeKeyspace(this.cassandraMapping.getKeyspaceName());
    return (keyspaceDefinition != null);
  }

  /**
   * Check if plausible keyspace already exists. If not, create it.
   */
  public void checkKeyspace() {
    KeyspaceDefinition keyspaceDefinition = this.cluster.describeKeyspace(this.cassandraMapping.getKeyspaceName());

    if (keyspaceDefinition == null) {
      // load keyspace definition
      List<ColumnFamilyDefinition> columnFamilyDefinitions = this.cassandraMapping.getColumnFamilyDefinitions();
      keyspaceDefinition = HFactory.createKeyspaceDefinition(this.cassandraMapping.getKeyspaceName(), this.cassandraMapping.getReplicationStrategy(),
          this.cassandraMapping.getReplicationFactor(), columnFamilyDefinitions);

      // create keyspace on server
      this.cluster.addKeyspace(keyspaceDefinition, true);
      LOG.info("Keyspace '" + this.cassandraMapping.getKeyspaceName() + "' in cluster '" + this.cassandraMapping.getClusterName() + "' was created on host '"
          + this.cassandraMapping.getHostName() + "'");
    } else {
      // simple plausibility test for keyspace corruption
      List<ColumnFamilyDefinition> cfDefs = keyspaceDefinition.getCfDefs();
      if (cfDefs == null || cfDefs.size() == 0)
        LOG.warn(keyspaceDefinition.getName() + " does not contain any column families.");
    }// if
  }

  /**
   * Drop keyspace.
   */
  public void dropKeyspace() {
    // "drop keyspace <keyspaceName>;" query
    this.cluster.dropKeyspace(this.cassandraMapping.getKeyspaceName());
  }

  /**
   * Insert a field in a column.
   *
   * @param key
   *          the row key
   * @param fieldName
   *          the field name
   * @param value
   *          the field value.
   */
  public void addColumn(PK key, String fieldName, Object value) {
    if (value == null) {
      return;
    }

    // map complex rowKey and ColumnName
    DynamicComposite colKey;
    DynamicComposite rowKey;
    try {
      colKey = this.keyMapper.getColumnName(key, fieldName, true);
      rowKey = this.keyMapper.getRowKey(key);
    } catch (RuntimeException re) {
      LOG.error("Error while mapping keys. Value was not persisted.", re);
      return;
    }

    String columnFamily = this.cassandraMapping.getFamily(fieldName);
    ByteBuffer byteBuffer = toByteBuffer(value);

    synchronized (mutator) {
      HectorUtils.insertColumn(mutator, rowKey, columnFamily, colKey, byteBuffer);
    }
  }

  /**
   * Insert a member in a super column. This might be used for map and record Avro types.
   *
   * @param key
   *          the row key
   * @param fieldName
   *          the field name
   * @param columnName
   *          the column name (the member name, or the index of array)
   * @param value
   *          the member value
   */
  public void addSubColumn(PK key, String fieldName, ByteBuffer columnName, Object value) {
    if (value == null) {
      return;
    }

    // map complex rowKey and ColumnName
    DynamicComposite colKey;
    DynamicComposite rowKey;
    try {
      colKey = this.keyMapper.getColumnName(key, fieldName, true);
      rowKey = this.keyMapper.getRowKey(key);
    } catch (RuntimeException re) {
      LOG.error("Error while mapping keys. Value was not persisted.", re);
      return;
    }

    String columnFamily = this.cassandraMapping.getFamily(fieldName);
    ByteBuffer byteBuffer = toByteBuffer(value);

    synchronized (mutator) {
      HectorUtils.insertSubColumn(mutator, rowKey, columnFamily, colKey, columnName, byteBuffer);
    }
  }

  /**
   * Adds an subColumn inside the cassandraMapping file when a String is serialized
   *
   * @param key
   * @param fieldName
   * @param columnName
   * @param value
   */
  public void addSubColumn(PK key, String fieldName, String columnName, Object value) {
    addSubColumn(key, fieldName, StringSerializer.get().toByteBuffer(columnName), value);
  }

  /**
   * Adds an subColumn inside the cassandraMapping file when an Integer is serialized
   *
   * @param key
   * @param fieldName
   * @param columnName
   * @param value
   */
  public void addSubColumn(PK key, String fieldName, Integer columnName, Object value) {
    addSubColumn(key, fieldName, IntegerSerializer.get().toByteBuffer(columnName), value);
  }

  /**
   * Delete a member in a super column. This is used for map and record Avro types.
   *
   * @param key
   *          the row key
   * @param fieldName
   *          the field name
   * @param columnName
   *          the column name (the member name, or the index of array)
   */
  public void deleteSubColumn(PK key, String fieldName, ByteBuffer columnName) {
    // map complex rowKey and ColumnName
    DynamicComposite colKey;
    DynamicComposite rowKey;
    try {
      colKey = this.keyMapper.getColumnName(key, fieldName, true); // TODO check
      rowKey = this.keyMapper.getRowKey(key);
    } catch (RuntimeException re) {
      LOG.error("Error while mapping keys. Value was not persisted.", re);
      return;
    }

    String columnFamily = this.cassandraMapping.getFamily(fieldName);

    synchronized (mutator) {
      HectorUtils.deleteSubColumn(mutator, rowKey, columnFamily, colKey, columnName);
    }
  }

  public void deleteSubColumn(PK key, String fieldName, String columnName) {
    deleteSubColumn(key, fieldName, StringSerializer.get().toByteBuffer(columnName));
  }

  public void addGenericArray(PK key, String fieldName, GenericArray array) {
    if (isSuper(cassandraMapping.getFamily(fieldName))) {
      int i = 0;
      for (Object itemValue : array) {

        // TODO: hack, do not store empty arrays
        if (itemValue instanceof GenericArray<?>) {
          if (((GenericArray) itemValue).size() == 0) {
            continue;
          }
        } else if (itemValue instanceof StatefulHashMap<?, ?>) {
          if (((StatefulHashMap) itemValue).size() == 0) {
            continue;
          }
        }

        addSubColumn(key, fieldName, i++, itemValue);
      }
    } else {
      addColumn(key, fieldName, array);
    }
  }

  public void addStatefulHashMap(PK key, String fieldName, StatefulHashMap<Utf8, Object> map) {
    if (isSuper(cassandraMapping.getFamily(fieldName))) {
      for (Utf8 mapKey : map.keySet()) {
        if (map.getState(mapKey) == State.DELETED) {
          deleteSubColumn(key, fieldName, mapKey.toString());
          continue;
        }

        // TODO: hack, do not store empty arrays
        Object mapValue = map.get(mapKey);
        if (mapValue instanceof GenericArray<?>) {
          if (((GenericArray) mapValue).size() == 0) {
            continue;
          }
        } else if (mapValue instanceof StatefulHashMap<?, ?>) {
          if (((StatefulHashMap) mapValue).size() == 0) {
            continue;
          }
        }

        addSubColumn(key, fieldName, mapKey.toString(), mapValue);
      }
    } else {
      addColumn(key, fieldName, map);
    }
  }

  /**
   * Serialize value to ByteBuffer.
   *
   * @param value
   *          the member value
   * @return ByteBuffer object
   */
  @SuppressWarnings("unchecked")
  public ByteBuffer toByteBuffer(Object value) {
    ByteBuffer byteBuffer = null;

    @SuppressWarnings("rawtypes")
    Serializer serializer = GoraSerializerTypeInferer.getSerializer(value);
    if (serializer == null) {
      LOG.info("Serializer not found for: " + value.toString());
    } else {
      byteBuffer = serializer.toByteBuffer(value);
    }

    if (byteBuffer == null) {
      LOG.warn("value class=" + value.getClass().getName() + " value=" + value + " -> null");
    }

    return byteBuffer;
  }

  private CassandraQueryType getQueryType(PK startKey, PK endKey) {
    boolean isColScan = keyMapper.isCassandraColumnScan(startKey, endKey);
    boolean isRowScan = keyMapper.isCassandraRowScan(startKey, endKey);

    if (!keyMapper.isPersistentPrimaryKey()) {
      if (isRowScan)
        return CassandraQueryType.ROWSCAN_PRIMITIVE;
      else
        return CassandraQueryType.SINGLE_PRIMITIVE;
    }

    if (isColScan && isRowScan)
      return CassandraQueryType.MULTISCAN;
    if (isColScan)
      return CassandraQueryType.COLUMNSCAN;
    if (isRowScan)
      return CassandraQueryType.ROWSCAN;
    return CassandraQueryType.SINGLE;
  }

  private int[] getQueryLimits(CassandraQueryType qType, long qLimit) {
    int[] result = new int[2];

    // get num of fields
    int numOfFields = 0;
    try {
      numOfFields = persistentClass.newInstance().getSchema().getFields().size();
    } catch (Exception e) {
      LOG.error("Unable to process persistent class.", e);
    }

    int limit = (int) qLimit;
    if (limit < 1) {
      limit = GoraRecordReader.BUFFER_LIMIT_READ_VALUE;
    }

    int columnCount = 0;
    int rowCount = 0;
    switch (qType) {
    case MULTISCAN: // unlikely case
      columnCount = limit * numOfFields;
      rowCount = limit;
      break;
    case COLUMNSCAN:
      columnCount = limit * numOfFields;
      rowCount = 1;
      break;
    case ROWSCAN:
      columnCount = numOfFields;
      rowCount = limit;
      break;
    case SINGLE:
      columnCount = numOfFields;
      rowCount = 1;
    case ROWSCAN_PRIMITIVE:
      columnCount = numOfFields;
      rowCount = limit;
      break;
    case SINGLE_PRIMITIVE:
      columnCount = numOfFields;
      rowCount = 1;
      break;
    default:
      break;
    }

    result[0] = columnCount;
    result[1] = rowCount;

    return result;
  }

  /**
   * Create and execute hector query
   *
   * @param cassandraQuery
   *          a wrapper of the query
   * @param family
   *          the family name to be queried
   * @return a list of family rows
   */
  public List<Row<DynamicComposite, DynamicComposite, ByteBuffer>> execute(CassandraQuery<PK, T> cassandraQuery, String family) {
    // analyze query
    Query<PK, T> query = cassandraQuery.getQuery();
    CassandraQueryType queryType = getQueryType(query.getStartKey(), query.getEndKey());

    // deduce result counts
    int[] counts = getQueryLimits(queryType, query.getLimit());

    // set up row key range
    DynamicComposite startKey = keyMapper.getRowKey(query.getStartKey());
    DynamicComposite endKey = keyMapper.getRowKey(query.getEndKey());
    int rowCount = counts[1];

    // set up slice predicate
    DynamicComposite startName = keyMapper.getColumnName(query.getStartKey(), FIELD_SCAN_COLUMN_RANGE_DELIMITER_START, false);
    DynamicComposite endName = keyMapper.getColumnName(query.getEndKey(), FIELD_SCAN_COLUMN_RANGE_DELIMITER_END, false);
    int columnCount = counts[0];

    // set up cassandra query
    RangeSlicesQuery<DynamicComposite, DynamicComposite, ByteBuffer> rangeSlicesQuery = HFactory.createRangeSlicesQuery(this.keyspace,
        DynamicCompositeSerializer.get(), DynamicCompositeSerializer.get(), ByteBufferSerializer.get());

    rangeSlicesQuery.setColumnFamily(family);
    rangeSlicesQuery.setKeys(startKey, endKey);
    rangeSlicesQuery.setRange(startName, endName, false, columnCount);
    rangeSlicesQuery.setRowCount(rowCount);

    // fire off the query
    QueryResult<OrderedRows<DynamicComposite, DynamicComposite, ByteBuffer>> queryResult = rangeSlicesQuery.execute();
    OrderedRows<DynamicComposite, DynamicComposite, ByteBuffer> orderedRows = queryResult.get();

    return orderedRows.getList();
  }

  /**
   * Select the families that contain at least one column mapped to a query field.
   *
   * @param query
   *          indicates the columns to select
   * @return a map which keys are the family names and values the corresponding column names
   *         required to get all the query fields.
   */
  public Map<String, List<String>> getFamilyMap(Query<PK, T> query) {
    Map<String, List<String>> map = new HashMap<String, List<String>>();
    for (String field : query.getFields()) {
      String family = this.getMappingFamily(field);
      String column = this.getMappingColumn(field);

      // check if the family value was already initialized
      List<String> list = map.get(family);
      if (list == null) {
        list = new ArrayList<String>();
        map.put(family, list);
      }
      if (column != null)
        list.add(column);
    }// for
    return map;
  }

  public List<SuperRow<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer>> executeSuper(CassandraQuery<PK, T> cassandraQuery, String family) {
    // analyze query
    Query<PK, T> query = cassandraQuery.getQuery();
    CassandraQueryType queryType = getQueryType(query.getStartKey(), query.getEndKey());

    // deduce result counts
    int[] counts = getQueryLimits(queryType, query.getLimit());

    // set up row key range
    DynamicComposite startKey = keyMapper.getRowKey(query.getStartKey());
    DynamicComposite endKey = keyMapper.getRowKey(query.getEndKey());
    int rowCount = counts[1];

    // set up slice predicate
    DynamicComposite startName = keyMapper.getColumnName(query.getStartKey(), FIELD_SCAN_COLUMN_RANGE_DELIMITER_START, false);
    DynamicComposite endName = keyMapper.getColumnName(query.getEndKey(), FIELD_SCAN_COLUMN_RANGE_DELIMITER_END, false);
    int columnCount = counts[0];

    // set up cassandra query
    RangeSuperSlicesQuery<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer> rangeSuperSlicesQuery = HFactory.createRangeSuperSlicesQuery(
        this.keyspace, DynamicCompositeSerializer.get(), DynamicCompositeSerializer.get(), ByteBufferSerializer.get(), ByteBufferSerializer.get());

    rangeSuperSlicesQuery.setColumnFamily(family);
    rangeSuperSlicesQuery.setKeys(startKey, endKey);
    rangeSuperSlicesQuery.setRange(startName, endName, false, columnCount);
    rangeSuperSlicesQuery.setRowCount(rowCount);

    // fire off the query
    QueryResult<OrderedSuperRows<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer>> queryResult = rangeSuperSlicesQuery.execute();
    OrderedSuperRows<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer> orderedRows = queryResult.get();

    return orderedRows.getList();
  }

  private String getMappingFamily(String pField) {
    String family = null;
    // TODO checking if it was a UNION field the one we are retrieving
    family = this.cassandraMapping.getFamily(pField);
    return family;
  }

  private String getMappingColumn(String pField) {
    String column = null;
    // TODO checking if it was a UNION field the one we are retrieving e.g. column = pField;
    column = this.cassandraMapping.getColumn(pField);
    return column;
  }

  /**
   * Retrieves the cassandraMapping which holds whatever was mapped from the
   * gora-cassandra-mapping.xml
   *
   * @return
   */
  public CassandraMapping getCassandraMapping() {
    return this.cassandraMapping;
  }

  /**
   * Select the field names according to the column names, which format if fully qualified:
   * "family:column"
   *
   * TODO needed?
   *
   * @param query
   * @return a map which keys are the fully qualified column names and values the query fields
   */
  public Map<String, String> getReverseMap(Query<PK, T> query) {
    Map<String, String> map = new HashMap<String, String>();
    for (String field : query.getFields()) {
      String family = this.getMappingFamily(field);
      String column = this.getMappingColumn(field);

      map.put(family + ":" + column, field);
    }

    return map;
  }

  public boolean isSuper(String family) {
    return this.cassandraMapping.isSuper(family);
  }

  public String getKeyspaceName() {
    return this.cassandraMapping.getKeyspaceName();
  }

  public CassandraKeyMapper<PK, T> getKeyMapper() {
    return keyMapper;
  }

  public void setKeyMapper(CassandraKeyMapper<PK, T> keyMapper) {
    this.keyMapper = keyMapper;
  }

  public Class<PK> getPrimaryKeyClass() {
    return primaryKeyClass;
  }

  public void setPrimaryKeyClass(Class<PK> primaryKeyClass) {
    this.primaryKeyClass = primaryKeyClass;
  }

  public Class<T> getPersistentClass() {
    return persistentClass;
  }

  public void setPersistentClass(Class<T> persistentClass) {
    this.persistentClass = persistentClass;
  }
}
