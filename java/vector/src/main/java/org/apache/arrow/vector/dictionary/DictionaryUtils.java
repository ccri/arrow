/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.arrow.vector.dictionary;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

public class DictionaryUtils {

  // TODO recursively examine fields?

  /**
   * Dictionary encodes a vector with a provided dictionary. The dictionary must contain all values in the vector.
   *
   * @param vector vector to encode
   * @param dictionary dictionary used for encoding
   * @return dictionary encoded vector
   */
  public static ValueVector encode(ValueVector vector, Dictionary dictionary) {
    validateType(vector.getMinorType());
    // load dictionary values into a hashmap for lookup
    ValueVector.Accessor dictionaryAccessor = dictionary.getVector().getAccessor();
    Map<Object, Integer> lookUps = new HashMap<>(dictionaryAccessor.getValueCount());
    for (int i = 0; i < dictionaryAccessor.getValueCount(); i++) {
      // for primitive array types we need a wrapper that implements equals and hashcode appropriately
      lookUps.put(dictionaryAccessor.getObject(i), i);
    }

    Field valueField = vector.getField();
    Field indexField = new Field(valueField.getName(), valueField.isNullable(),
      dictionary.getEncoding().getIndexType(), dictionary.getEncoding(), null);

    // vector to hold our indices (dictionary encoded values)
    FieldVector indices = indexField.createVector(vector.getAllocator());
    ValueVector.Mutator mutator = indices.getMutator();

    // use reflection to pull out the set method
    // TODO implement a common interface for int vectors
    Method setter = null;
    for (Class<?> c: ImmutableList.of(int.class, long.class)) {
      try {
        setter = mutator.getClass().getMethod("set", int.class, c);
        break;
      } catch(NoSuchMethodException e) {
        // ignore
      }
    }
    if (setter == null) {
      throw new IllegalArgumentException("Dictionary encoding does not have a valid int type");
    }

    ValueVector.Accessor accessor = vector.getAccessor();
    int count = accessor.getValueCount();

    indices.allocateNew();

    try {
      for (int i = 0; i < count; i++) {
        Object value = accessor.getObject(i);
        if (value != null) { // if it's null leave it null
          // note: this may fail if value was not included in the dictionary
          setter.invoke(mutator, i, lookUps.get(value));
        }
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    mutator.setValueCount(count);

    return indices;
  }

  /**
   * Decodes a dictionary encoded array using the provided dictionary.
   *
   * @param indices dictionary encoded values, must be int type
   * @param dictionary dictionary used to decode the values
   * @return vector with values restored from dictionary
   */
  public static ValueVector decode(ValueVector indices, Dictionary dictionary) {
    ValueVector.Accessor accessor = indices.getAccessor();
    int count = accessor.getValueCount();
    ValueVector dictionaryVector = dictionary.getVector();
    // copy the dictionary values into the decoded vector
    TransferPair transfer = dictionaryVector.getTransferPair(indices.getAllocator());
    transfer.getTo().allocateNewSafe();
    for (int i = 0; i < count; i++) {
      Object index = accessor.getObject(i);
      if (index != null) {
        transfer.copyValueSafe(((Number) index).intValue(), i);
      }
    }
    // TODO do we need to worry about the field?
    ValueVector decoded = transfer.getTo();
    decoded.getMutator().setValueCount(count);
    return decoded;
  }

  private static void validateType(MinorType type) {
    // byte arrays don't work as keys in our dictionary map - we could wrap them with something to
    // implement equals and hashcode if we want that functionality
    if (type == MinorType.VARBINARY || type == MinorType.LIST || type == MinorType.MAP || type == MinorType.UNION) {
      throw new IllegalArgumentException("Dictionary encoding for complex types not implemented");
    }
  }
}
