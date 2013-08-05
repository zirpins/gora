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

import me.prettyprint.hector.api.Serializer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.gora.cassandra.serializers.GoraSerializerTypeInferer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an abstract name/value pair. Column name types are generic. Values might be atomic or composed.
 *
 * @param CN
 *          column name type
 */
public abstract class CassandraColumn<CN> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraColumn.class);

  private String family;
  private int type;
  private Field field;
  private int unionType;

  public abstract CN getName();

  public abstract Object getValue();

  protected Object fromByteBuffer(Schema schema, ByteBuffer byteBuffer) {
    Object value = null;
    Serializer<?> serializer = GoraSerializerTypeInferer.getSerializer(schema);
    if (serializer == null) {
      LOG.info("Schema is not supported: " + schema.toString());
    } else {
      value = serializer.fromByteBuffer(byteBuffer);
    }
    return value;
  }

  public void setUnionType(int pUnionType) {
    this.unionType = pUnionType;
  }

  public int getUnionType() {
    return unionType;
  }

  public String getFamily() {
    return family;
  }

  public void setFamily(String family) {
    this.family = family;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public void setField(Field field) {
    this.field = field;
  }

  protected Field getField() {
    return this.field;
  }

}
