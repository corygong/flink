/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.distributedlog;

import com.twitter.distributedlog.DLSN;
import org.apache.flink.streaming.connectors.distributedlog.internal.DistributedLogOffsetStore;
import org.apache.flink.streaming.connectors.distributedlog.model.DistributedLogStream;

import java.util.HashMap;
import java.util.Map;

public class DistributedLogMemoryOffsetStore implements DistributedLogOffsetStore {
	private final static Map<DistributedLogStream, DLSN> cache = new HashMap<>();

	@Override
	public void open() {
	}

	@Override
	public void close() {
		cache.clear();
	}

	@Override
	public void commitDLSNOffset(DistributedLogStream stream, DLSN offset) {
		cache.put(stream,offset);
	}

	@Override
	public DLSN getLatestDLSNOffset(DistributedLogStream stream) {
		return cache.get(stream);
	}
}
