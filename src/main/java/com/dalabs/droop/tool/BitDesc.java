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

package com.dalabs.droop.tool;

/**
 * Describes a DroopBit.
 * This class should be final
 */
public class BitDesc {
  private final String bitName;
  private final Class<? extends DroopBit> bitClass;
  private final String description;


  /**
   * Main c'tor; sets all fields that describe a DroopBit.
   */
  public BitDesc(String name, Class<? extends DroopBit> cls, String desc) {
    this.bitName = name;
    this.bitClass = cls;
    this.description = desc;
  }

  /**
   * @return the name used to invoke the bit (e.g., 'droop &lt;foo&gt;')
   */
  public String getName() {
    return bitName;
  }

  /**
   * @return a human-readable description of what the bit does.
   */
  public String getDesc() {
    return description;
  }

  /**
   * @return the class that implements DroopBit.
   */
  public Class<? extends DroopBit> getBitClass() {
    return bitClass;
  }

}
