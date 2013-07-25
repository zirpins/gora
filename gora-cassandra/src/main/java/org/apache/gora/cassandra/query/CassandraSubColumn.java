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

package org.apache.gora.cassandra.query;

import java.nio.ByteBuffer;

import me.prettyprint.hector.api.beans.HColumn;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.gora.cassandra.serializers.GenericArraySerializer;
import org.apache.gora.cassandra.serializers.StatefulHashMapSerializer;
import org.apache.gora.cassandra.store.CassandraStore;
import org.apache.gora.persistency.StatefulHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an abstract name/value pair. Column name types are generic. Values are atomic.
 *
 * @param CN
 *          column name type
 */
public class CassandraSubColumn<CN> extends CassandraColumn<CN> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSubColumn.class);

  // Hector column holding the data
  private HColumn<CN, ByteBuffer> hColumn;

  @Override
  public CN getName() {
    return hColumn.getName();
  }

  public void setValue(HColumn<CN, ByteBuffer> hColumn) {
    this.hColumn = hColumn;
  }

  protected ByteBuffer getBytes() {
    return hColumn.getValue();
  }

  /**
   * Deserialize byteBuffer into a typed Object, according to the field schema.
   * @see org.apache.gora.cassandra.query.CassandraColumn#getValue()
   */
  @SuppressWarnings("rawtypes")
  public Object getValue() {
    Field field = getField();
    Schema fieldSchema = field.schema();
    Type type = fieldSchema.getType();

    ByteBuffer byteBuffer = getBytes();

    if (byteBuffer == null) {
      LOG.debug("Column " + toString() + " is null.");
      return null;
    }

    Object value = null;

    if (type == Type.ARRAY) {
      GenericArraySerializer serializer = GenericArraySerializer.get(fieldSchema.getElementType());
      GenericArray genericArray = serializer.fromByteBuffer(byteBuffer);
      value = genericArray;
    } else if (type == Type.MAP) {
      StatefulHashMapSerializer serializer = StatefulHashMapSerializer.get(fieldSchema.getValueType());
      StatefulHashMap map = serializer.fromByteBuffer(byteBuffer);
      value = map;
    } else if (type == Type.UNION){
      // the selected union schema is obtained
      Schema unionFieldSchema = getUnionSchema(super.getUnionType(), field.schema());
      // we use the selected union schema to deserialize our actual value
      value = fromByteBuffer(unionFieldSchema, byteBuffer);
    } else {
      value = fromByteBuffer(fieldSchema, byteBuffer);
    }

    return value;
  }

  /**
   * Gets the specific schema for a union data type
   * @param pSchemaPos
   * @param pSchema
   * @return
   */
  protected Schema getUnionSchema (int pSchemaPos, Schema pSchema){
    Schema unionSchema = pSchema.getTypes().get(pSchemaPos);
    // default union element
    if ( unionSchema == null )
      pSchema.getTypes().get(CassandraStore.DEFAULT_UNION_SCHEMA);
    return unionSchema;
  }

}
