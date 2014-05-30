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
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * Implements the {@link DNSToSwitchMapping} via static mappings. Used in
 * testcases that simulate racks, and in the
 * {@link org.apache.hadoop.hdfs.MiniDFSCluster}
 * 
 * A shared, static mapping is used; to reset it call {@link #resetMap()}.
 * 
 * When an instance of the class has its {@link #setConf(Configuration)} method
 * called, nodes listed in the configuration will be added to the map. These do
 * not get removed when the instance is garbage collected.
 * 
 * The switch mapping policy of this class is the same as for the
 * {@link ScriptBasedMapping} -the presence of a non-empty topology script. The
 * script itself is not used.
 */
public class CachedStaticMapping extends CachedDNSToSwitchMapping {

	/**
	 * Create an instance with the default configuration. </p> Calling
	 * {@link #setConf(Configuration)} will trigger a re-evaluation of the
	 * configuration settings and so be used to set up the mapping script.
	 * 
	 */
	public CachedStaticMapping() {
		this(new RawStaticMapping());
	}

	/**
	 * Create an instance from the given raw mapping
	 * 
	 * @param rawMap
	 *            raw DNSTOSwithMapping
	 */
	public CachedStaticMapping(DNSToSwitchMapping rawMap) {
		super(rawMap);
	}

	/**
	 * Get the cached mapping and convert it to its real type
	 * 
	 * @return the inner raw script mapping.
	 */
	public RawStaticMapping getRawMapping() {
		return (RawStaticMapping) rawMapping;
	}

	public static class RawStaticMapping implements DNSToSwitchMapping {

		/* Only one instance per JVM */
		private static final Map<String, String> nameToRackMap = new HashMap<String, String>();

		/**
		 * Add a node to the static map. The moment any entry is added to the
		 * map, the map goes multi-rack.
		 * 
		 * @param name
		 *            node name
		 * @param rackId
		 *            rack ID
		 */
		public static void addNodeToRack(String name, String rackId) {
			nameToRackMap.put(name, rackId);
		}

		@Override
		public List<String> resolve(List<String> names) {

			List<String> m = new ArrayList<String>();

			for (String name : names) {
				String rackId;
				if ((rackId = nameToRackMap.get(name)) != null) {
					m.add(rackId);
				} else {
					m.add(NetworkTopology.DEFAULT_RACK);
				}
			}

			return m;
		}

		/**
		 * Clear the map
		 */
		public static void resetMap() {
			synchronized (nameToRackMap) {
				nameToRackMap.clear();
			}
		}

		public void reloadCachedMappings() {
			// reloadCachedMappings does nothing for StaticMapping; there is
			// nowhere to reload from since all data is in memory.
		}

		@Override
		public void reloadCachedMappings(List<String> names) {
			// reloadCachedMappings does nothing for StaticMapping; there is
			// nowhere to reload from since all data is in memory.
		}
	}

}
