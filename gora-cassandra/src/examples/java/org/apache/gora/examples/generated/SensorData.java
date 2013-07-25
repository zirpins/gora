/**
 *Licensed to the Apache Software Foundation (ASF) under one
 *or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 *regarding copyright ownership.  The ASF licenses this file
 *to you under the Apache License, Version 2.0 (the"
 *License"); you may not use this file except in compliance
 *with the License.  You may obtain a copy of the License at
 *
  * http://www.apache.org/licenses/LICENSE-2.0
 * 
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */

package org.apache.gora.examples.generated;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;

@SuppressWarnings("all")
public class SensorData extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"SensorData\",\"namespace\":\"org.apache.gora.examples.generated\",\"fields\":[{\"name\":\"reading\",\"type\":\"double\"},{\"name\":\"events\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"params\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"context\",\"type\":{\"type\":\"record\",\"name\":\"SensorContext\",\"fields\":[{\"name\":\"mem\",\"type\":\"double\"},{\"name\":\"power\",\"type\":\"double\"}]}}]}");
  public static enum Field {
    READING(0,"reading"),
    EVENTS(1,"events"),
    PARAMS(2,"params"),
    CONTEXT(3,"context"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"reading","events","params","context",};
  static {
    PersistentBase.registerFields(SensorData.class, _ALL_FIELDS);
  }
  private double reading;
  private GenericArray<Integer> events;
  private Map<Utf8,Utf8> params;
  private SensorContext context;
  public SensorData() {
    this(new StateManagerImpl());
  }
  public SensorData(StateManager stateManager) {
    super(stateManager);
    events = new ListGenericArray<Integer>(getSchema().getField("events").schema());
    params = new StatefulHashMap<Utf8,Utf8>();
  }
  public SensorData newInstance(StateManager stateManager) {
    return new SensorData(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return reading;
    case 1: return events;
    case 2: return params;
    case 3: return context;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:reading = (Double)_value; break;
    case 1:events = (GenericArray<Integer>)_value; break;
    case 2:params = (Map<Utf8,Utf8>)_value; break;
    case 3:context = (SensorContext)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public double getReading() {
    return (Double) get(0);
  }
  public void setReading(double value) {
    put(0, value);
  }
  public GenericArray<Integer> getEvents() {
    return (GenericArray<Integer>) get(1);
  }
  public void addToEvents(int element) {
    getStateManager().setDirty(this, 1);
    events.add(element);
  }
  public Map<Utf8, Utf8> getParams() {
    return (Map<Utf8, Utf8>) get(2);
  }
  public Utf8 getFromParams(Utf8 key) {
    if (params == null) { return null; }
    return params.get(key);
  }
  public void putToParams(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 2);
    params.put(key, value);
  }
  public Utf8 removeFromParams(Utf8 key) {
    if (params == null) { return null; }
    getStateManager().setDirty(this, 2);
    return params.remove(key);
  }
  public SensorContext getContext() {
    return (SensorContext) get(3);
  }
  public void setContext(SensorContext value) {
    put(3, value);
  }
}
