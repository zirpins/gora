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
public class SensorContext extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"SensorContext\",\"namespace\":\"org.apache.gora.examples.generated\",\"fields\":[{\"name\":\"mem\",\"type\":\"double\"},{\"name\":\"power\",\"type\":\"double\"}]}");
  public static enum Field {
    MEM(0,"mem"),
    POWER(1,"power"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"mem","power",};
  static {
    PersistentBase.registerFields(SensorContext.class, _ALL_FIELDS);
  }
  private double mem;
  private double power;
  public SensorContext() {
    this(new StateManagerImpl());
  }
  public SensorContext(StateManager stateManager) {
    super(stateManager);
  }
  public SensorContext newInstance(StateManager stateManager) {
    return new SensorContext(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return mem;
    case 1: return power;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:mem = (Double)_value; break;
    case 1:power = (Double)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public double getMem() {
    return (Double) get(0);
  }
  public void setMem(double value) {
    put(0, value);
  }
  public double getPower() {
    return (Double) get(1);
  }
  public void setPower(double value) {
    put(1, value);
  }
}
