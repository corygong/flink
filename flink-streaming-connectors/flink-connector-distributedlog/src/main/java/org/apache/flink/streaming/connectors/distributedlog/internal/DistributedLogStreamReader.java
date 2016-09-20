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

package org.apache.flink.streaming.connectors.distributedlog.internal;

import com.twitter.distributedlog.DLSN;
import org.apache.flink.streaming.connectors.distributedlog.model.DistributedLogStream;

import java.util.HashMap;
import java.util.Map;

/**
 * Multiple Streams Reader
 */
public interface DistributedLogStreamReader<T> {

	/**
	 * Snapshot read offset state
	 */
	HashMap<DistributedLogStream, DLSN> snapshotDLSNOffsets();

	/**
	 * Restore read offset state
	 */
	void restoreDLSNOffsets(HashMap<DistributedLogStream, DLSN> snapshot) throws Exception;

	/**
	 * Commit stream processed offset to persistence store.
	 */
	void commitDLSNOffsets(HashMap<DistributedLogStream, DLSN> offset);

	/**
	 * Consume data if having any new data.
	 */
	void runReadLoop();

	void cancel();
}
