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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperSlice;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.gora.cassandra.query.CassandraQuery;
import org.apache.gora.cassandra.query.CassandraResult;
import org.apache.gora.cassandra.query.CassandraResultList;
import org.apache.gora.cassandra.query.CassandraSubColumn;
import org.apache.gora.cassandra.query.CassandraMixedRow;
import org.apache.gora.cassandra.query.CassandraSuperColumn;
import org.apache.gora.persistency.ListGenericArray;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.GoraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.apache.gora.cassandra.store.CassandraStore} is the primary class responsible for
 * directing Gora CRUD operations into Cassandra. We (delegate) rely heavily on @ link
 * org.apache.gora.cassandra.store.CassandraClient} for many operations such as initialization,
 * creating and deleting schemas (Cassandra Keyspaces), etc.
 *
 * @param PK
 *          primary key class (not cassandra row key)
 * @param T
 *          persistent class
 *
 */
public class CassandraStore<PK, T extends PersistentBase> extends DataStoreBase<PK, T> {

  /** Logging implementation */
  public static final Logger LOG = LoggerFactory.getLogger(CassandraStore.class);

  private CassandraClient<PK, T> cassandraClient = new CassandraClient<PK, T>();

  private CassandraKeyMapper<PK, T> keyMapper;

  /**
   * Default schema index used when AVRO Union data types are stored
   */
  public static int DEFAULT_UNION_SCHEMA = 0;

  /**
   * The values are Avro fields pending to be stored.
   *
   * We want to iterate over the keys in insertion order. We don't want to lock the entire
   * collection before iterating over the keys, since in the meantime other threads are adding
   * entries to the map.
   */
  private Map<PK, T> buffer = Collections.synchronizedMap(new LinkedHashMap<PK, T>());

  /**
   * Initialize is called when then the call to {@link
   * org.apache.gora.store.DataStoreFactory#createDataStore(Class<D> dataStoreClass, Class<K>
   * keyClass, Class<T> persistent, org.apache.hadoop.conf.Configuration conf)} is made. In this
   * case, we merely delegate the store initialization to the {@link
   * org.apache.gora.cassandra.store.CassandraClient#initialize(Class<K> keyClass, Class<T>
   * persistentClass)}.
   */
  public void initialize(Class<PK> primaryKeyClass, Class<T> persistentClass, Properties properties) {
    try {
      super.initialize(primaryKeyClass, persistentClass, properties);
      this.cassandraClient.initialize(primaryKeyClass, persistentClass);
      this.keyMapper = cassandraClient.getKeyMapper();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      LOG.error(e.getStackTrace().toString());
    }
  }

  @Override
  public void close() {
    LOG.debug("close");
    flush();
  }

  @Override
  public void createSchema() {
    LOG.debug("creating Cassandra keyspace");
    this.cassandraClient.checkKeyspace();
  }

  @Override
  public boolean delete(PK key) {
    LOG.debug("delete " + key);
    return false;
  }

  @Override
  public long deleteByQuery(Query<PK, T> query) {
    LOG.debug("delete by query " + query);
    return 0;
  }

  @Override
  public void deleteSchema() {
    LOG.debug("delete schema");
    this.cassandraClient.dropKeyspace();
  }

  /**
   * When executing Gora Queries in Cassandra we query the Cassandra keyspace by families. When add
   * sub/supercolumns, Gora keys are mapped to Cassandra composite primary keys.
   */
  @Override
  public Result<PK, T> execute(Query<PK, T> query) {

    Map<String, List<String>> familyMap = this.cassandraClient.getFamilyMap(query);
    Map<String, String> reverseMap = this.cassandraClient.getReverseMap(query);

    CassandraQuery<PK, T> cassandraQuery = new CassandraQuery<PK, T>();
    cassandraQuery.setQuery(query);
    cassandraQuery.setFamilyMap(familyMap);

    CassandraResult<PK, T> cassandraResult = new CassandraResult<PK, T>(this, query);
    cassandraResult.setReverseMap(reverseMap);

    CassandraResultList<PK> cassandraResultList = new CassandraResultList<PK>();

    // We query Cassandra keyspace by families.
    for (String family : familyMap.keySet()) {
      if (family == null) {
        continue;
      }
      if (this.cassandraClient.isSuper(family)) {
        addSuperColumns(family, cassandraQuery, cassandraResultList);

      } else {
        addSubColumns(family, cassandraQuery, cassandraResultList);
      }
    }

    cassandraResult.setResultSet(cassandraResultList);

    return cassandraResult;
  }

  /**
   * When querying for columns, Gora keys are mapped to Cassandra Primary Keys consisting of
   * partition keys and column names. The Cassandra Primary Keys resulting from the query are mapped
   * back to Gora Keys. Each result row might contain clustered fields associated with multiple
   * persistent entities. Row columns are mapped to persistent entities by means of the clustering
   * information contained in their Gora key.
   */
  private void addSubColumns(String family, CassandraQuery<PK, T> cassandraQuery, CassandraResultList<PK> cassandraResultList) {
    // retrieve key range corresponding to the query parameters (triggers cassandra call)
    List<Row<DynamicComposite, DynamicComposite, ByteBuffer>> rows = this.cassandraClient.execute(cassandraQuery, family);

    // loop result rows
    for (Row<DynamicComposite, DynamicComposite, ByteBuffer> row : rows) {
      DynamicComposite compositeRowKey = row.getKey();
      ColumnSlice<DynamicComposite, ByteBuffer> columnSlice = row.getColumnSlice();

      // loop result columns
      for (HColumn<DynamicComposite, ByteBuffer> hColumn : columnSlice.getColumns()) {
        // extract complex primary key
        DynamicComposite compositeColumnName = hColumn.getName();
        PK partKey = cassandraClient.getKeyMapper().getPrimaryKey(compositeRowKey, compositeColumnName);

        // find associated slice in the result list
        CassandraMixedRow<PK> mixedRow = cassandraResultList.getMixedRow(partKey);
        if (mixedRow == null) {
          mixedRow = new CassandraMixedRow<PK>();
          cassandraResultList.putMixedRow(partKey, mixedRow);
          mixedRow.setKey(partKey);
        }

        // create column representation
        CassandraSubColumn<DynamicComposite> compositeColumn = new CassandraSubColumn<DynamicComposite>();
        compositeColumn.setValue(hColumn);
        compositeColumn.setFamily(family);

        // add column to slice
        mixedRow.add(compositeColumn);

      }// loop columns

    }// loop rows
  }

  /**
   * When querying for superColumns, Gora keys are mapped to Cassandra Primary Keys consisting of
   * partition keys and column names. The Cassandra Primary Keys resulting from the query are mapped
   * back to Gora Keys. Each result row might contain clustered fields associated with multiple
   * persistent entities. Row columns are mapped to persistent entities by means of the clustering
   * information contained in their Gora key.
   */
  private void addSuperColumns(String family, CassandraQuery<PK, T> cassandraQuery, CassandraResultList<PK> cassandraResultList) {
    // retrieve key range corresponding to the query parameters (triggers cassandra call)
    List<SuperRow<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer>> superRows = this.cassandraClient.executeSuper(cassandraQuery, family);

    // loop result rows
    for (SuperRow<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer> superRow : superRows) {
      DynamicComposite compositeSuperRowKey = superRow.getKey();
      SuperSlice<DynamicComposite, ByteBuffer, ByteBuffer> superSlice = superRow.getSuperSlice();

      // loop result columns
      for (HSuperColumn<DynamicComposite, ByteBuffer, ByteBuffer> hSuperColumn : superSlice.getSuperColumns()) {
        // extract complex primary key
        DynamicComposite compositeSuperColumnName = hSuperColumn.getName();
        PK partKey = cassandraClient.getKeyMapper().getPrimaryKey(compositeSuperRowKey, compositeSuperColumnName);

        // find associated slice in the result list
        CassandraMixedRow<PK> mixedRow = cassandraResultList.getMixedRow(partKey);
        if (mixedRow == null) {
          mixedRow = new CassandraMixedRow<PK>();
          cassandraResultList.putMixedRow(partKey, mixedRow);
          mixedRow.setKey(partKey);
        }

        // create column representation
        CassandraSuperColumn cassandraSuperColumn = new CassandraSuperColumn();
        cassandraSuperColumn.setValue(hSuperColumn);
        cassandraSuperColumn.setFamily(family);

        // add column to slice
        mixedRow.add(cassandraSuperColumn);

      }// loop superColumns

    }// loop superROws
  }

  /**
   * Flush the buffer. Write the buffered rows.
   *
   * @see org.apache.gora.store.DataStore#flush()
   */
  @Override
  public void flush() {
    Set<PK> keys = this.buffer.keySet();

    // this duplicates memory footprint
    @SuppressWarnings("unchecked")
    PK[] keyArray = (PK[]) keys.toArray();

    // iterating over the key set directly would throw ConcurrentModificationException with
    // java.util.HashMap and subclasses
    for (PK key : keyArray) {
      T value = this.buffer.get(key);
      if (value == null) {
        LOG.info("Value to update is null for key " + key);
        continue;
      }
      Schema schema = value.getSchema();
      for (Field field : schema.getFields()) {
        if (value.isDirty(field.pos())) {
          addOrUpdateField(key, field, value.get(field.pos()));
        }
      }
    }

    // remove flushed rows
    for (PK key : keyArray) {
      this.buffer.remove(key);
    }
  }

  @Override
  public T get(PK key, String[] fields) {
    CassandraQuery<PK, T> query = new CassandraQuery<PK, T>();
    query.setDataStore(this);
    query.setKeyRange(key, key);
    query.setFields(fields);
    query.setLimit(1);
    Result<PK, T> result = execute(query);
    boolean hasResult = false;
    try {
      hasResult = result.next();
    } catch (Exception e) {
      LOG.error("Error processing query result", e);
    }
    return hasResult ? result.get() : null;
  }

  /**
   * @see org.apache.gora.store.DataStore#getPartitions(org.apache.gora.query.Query)
   *
   *      the cassandra interpretation of this method relates to the partition components of the
   *      primary key. Queries with ranges in any part of the complex partition key would possibly
   *      need to traverse multiple nodes and are generally not possible with non order-preserving
   *      partitioners.
   *
   *      The method will check for partition key ranges and try to decompose them into a set of key
   *      pairs with absolute partition keys (preserving possible cluster key ranges). Such a
   *      decomposition is generally only feasible if the size of the associated key set is not too
   *      big. Therefore, only a few key types are supported including integer, boolean and long
   *      types.
   *
   *      Furthermore, for multi-dimensional partition keys, the range semantics differs from the
   *      default cassandra behavior. In cassandra, composite keys are stored in lexical order. This
   *      means that ranges of higher-level keys include the complete value ranges of lower-level
   *      keys (possibly slightly reduced by ranges of lower-level keys). Because this would result
   *      in far too many absolute keys (and associated queries), only the specified lower-level
   *      keys (absolute value or range) are considered. This means that rows with a lower-level key
   *      outside the specified range are not part of the query result, whereas with default
   *      cassandra behavior they would be included.
   *
   *      As a consequence, the automatic partitioning function is practically restricted to cases,
   *      where the data model is explicitly designed for it. For instance this works quite well for
   *      queries that consider modestly wide ranges of single components within a composite
   *      partition key.
   *
   *      If there is no partition key range or decomposition of partition keys is not possible, a
   *      single partition query will be returned with a warning.
   *
   *      Locations are not really relevant from a cassandra point of view, because performance
   *      gains from local queries are not likely to be significant. As a possible TODO, the default
   *      node (and possibly also replica nodes) associated with a partition key could be computed.
   *
   */
  @Override
  public List<PartitionQuery<PK, T>> getPartitions(Query<PK, T> query) throws IOException {
    // result list
    List<PartitionQuery<PK, T>> partitions = new ArrayList<PartitionQuery<PK, T>>();

    // a map holding key pairs for distinct partitions
    Map<PK, PK> keyMap = keyMapper.decomposePartionKeys(query.getStartKey(), query.getEndKey());

    // create a partition query for each partition key pair.
    for (Entry<PK, PK> e : keyMap.entrySet()) {
      partitions.add(new PartitionQueryImpl<PK, T>(query, e.getKey(), e.getValue()));
    }

    return partitions;
  }

  /**
   * In Cassandra Schemas are referred to as Keyspaces
   *
   * @return Keyspace
   */
  @Override
  public String getSchemaName() {
    return this.cassandraClient.getKeyspaceName();
  }

  @Override
  public Query<PK, T> newQuery() {
    Query<PK, T> query = new CassandraQuery<PK, T>(this);
    query.setFields(getFieldsToQuery(null));
    return query;
  }

  /**
   * Duplicate instance to keep all the objects in memory till flushing.
   *
   * @see org.apache.gora.store.DataStore#put(java.lang.Object,
   *      org.apache.gora.persistency.Persistent)
   */
  @Override
  @SuppressWarnings({ "rawtypes", "unchecked", "incomplete-switch" })
  public void put(PK key, T value) {
    T p = (T) value.newInstance(new StateManagerImpl());
    Schema schema = value.getSchema();

    for (Field field : schema.getFields()) {
      int fieldPos = field.pos();

      if (value.isDirty(fieldPos)) {
        Object fieldValue = value.get(fieldPos);

        // check if field has a nested structure (array, map, or record)
        Schema fieldSchema = field.schema();
        Type type = fieldSchema.getType();

        switch (type) {
        case RECORD:
          PersistentBase persistent = (PersistentBase) fieldValue;
          PersistentBase newRecord = (PersistentBase) persistent.newInstance(new StateManagerImpl());
          for (Field member : fieldSchema.getFields()) {
            newRecord.put(member.pos(), persistent.get(member.pos()));
          }
          fieldValue = newRecord;
          break;
        case MAP:
          StatefulHashMap map = (StatefulHashMap) fieldValue;
          StatefulHashMap newMap = new StatefulHashMap();
          for (Object mapKey : map.keySet()) {
            newMap.put(mapKey, map.get(mapKey));
            newMap.putState(mapKey, map.getState(mapKey));
          }
          fieldValue = newMap;
          break;
        case ARRAY:
          GenericArray array = (GenericArray) fieldValue;
          ListGenericArray newArray = new ListGenericArray(fieldSchema.getElementType());
          Iterator iter = array.iterator();
          while (iter.hasNext()) {
            newArray.add(iter.next());
          }
          fieldValue = newArray;
          break;
        case UNION:
          // XXX review UNION handling
          // storing the union selected schema, the actual value will be stored as soon as getting
          // out of here
          // TODO determine which schema we are using: int schemaPos =
          // getUnionSchema(fieldValue,fieldSchema);
          // and save it p.put( p.getFieldIndex(field.name() + CassandraStore.UNION_COL_SUFIX),
          // schemaPos);
          break;
        }

        p.put(fieldPos, fieldValue);

      }// if field dirty
    }// loop fields

    // this performs a structural modification of the map
    this.buffer.put(key, p);
  }

  /**
   * Add a field to Cassandra according to its type.
   *
   * @param key
   *          the key of the row where the field should be added
   * @param field
   *          the Avro field representing a datum
   * @param value
   *          the field value
   */
  private void addOrUpdateField(PK key, Field field, Object value) {
    Schema schema = field.schema();
    Type type = schema.getType();
    switch (type) {
    case STRING:
    case BOOLEAN:
    case INT:
    case LONG:
    case BYTES:
    case FLOAT:
    case DOUBLE:
    case FIXED:
      this.cassandraClient.addColumn(key, field.name(), value);
      break;
    case RECORD:
      if (value != null) {
        if (value instanceof PersistentBase) {
          PersistentBase persistentBase = (PersistentBase) value;
          for (Field member : schema.getFields()) {

            // TODO: hack, do not store empty arrays
            Object memberValue = persistentBase.get(member.pos());
            if (memberValue instanceof GenericArray<?>) {
              if (((GenericArray) memberValue).size() == 0) {
                continue;
              }
            }
            this.cassandraClient.addSubColumn(key, field.name(), member.name(), memberValue);
          }
        } else {
          LOG.info("Record not supported: " + value.toString());

        }
      }
      break;
    case MAP:
      if (value != null) {
        if (value instanceof StatefulHashMap<?, ?>) {
          this.cassandraClient.addStatefulHashMap(key, field.name(), (StatefulHashMap<Utf8, Object>) value);
        } else {
          LOG.info("Map not supported: " + value.toString());
        }
      }
      break;
    case ARRAY:
      if (value != null) {
        if (value instanceof GenericArray<?>) {
          this.cassandraClient.addGenericArray(key, field.name(), (GenericArray) value);
        } else {
          LOG.info("Array not supported: " + value.toString());
        }
      }
      break;
    case UNION:
      if (value != null) {
        LOG.info("Union being supported with value: " + value.toString());
        // XXX review UNION handling
        // TODO add union schema index used
        // adding union value
        this.cassandraClient.addColumn(key, field.name(), value);
      } else {
        LOG.info("Union not supported: " + value.toString());
      }
    default:
      LOG.info("Type not considered: " + type.name());
    }
  }

  /**
   * Gets the position within the schema of the type used
   *
   * @param pValue
   * @param pUnionSchema
   * @return
   */
  // XXX review UNION handling
  private int getUnionSchema(Object pValue, Schema pUnionSchema) {
    int unionSchemaPos = 0;
    String valueType = pValue.getClass().getSimpleName();
    Iterator<Schema> it = pUnionSchema.getTypes().iterator();
    while (it.hasNext()) {
      String schemaName = it.next().getName();
      if (valueType.equals("Utf8") && schemaName.equals(Type.STRING.name().toLowerCase()))
        return unionSchemaPos;
      else if (valueType.equals("HeapByteBuffer") && schemaName.equals(Type.STRING.name().toLowerCase()))
        return unionSchemaPos;
      else if (valueType.equals("Integer") && schemaName.equals(Type.INT.name().toLowerCase()))
        return unionSchemaPos;
      else if (valueType.equals("Long") && schemaName.equals(Type.LONG.name().toLowerCase()))
        return unionSchemaPos;
      else if (valueType.equals("Double") && schemaName.equals(Type.DOUBLE.name().toLowerCase()))
        return unionSchemaPos;
      else if (valueType.equals("Float") && schemaName.equals(Type.FLOAT.name().toLowerCase()))
        return unionSchemaPos;
      else if (valueType.equals("Boolean") && schemaName.equals(Type.BOOLEAN.name().toLowerCase()))
        return unionSchemaPos;
      unionSchemaPos++;
    }
    // if we weren't able to determine which data type it is, then we return the default
    return 0;
  }

  @Override
  public boolean schemaExists() {
    LOG.info("schema exists");
    return cassandraClient.keyspaceExists();
  }

  public CassandraKeyMapper<PK, T> getKeyMapper() {
    return this.keyMapper;
  }
}
