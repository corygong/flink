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

import com.twitter.distributedlog.BKDistributedLogNamespace;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import static com.twitter.distributedlog.LocalDLMEmulator.DLOG_NAMESPACE;
import static org.junit.Assert.assertTrue;

@PrepareForTest({DistributedLogSource.class})
public class DistributedLogSourceTest extends DistributedLogTestBase {

	@Test
	public void testOpenNamespace() throws URISyntaxException, IOException {
		DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
			.conf(new DistributedLogConfiguration())
			.uri(new URI("distributedlog-bk://" + zkServers + DLOG_NAMESPACE + "/bknamespace"))
			.build();
		try {
			assertTrue("distributedlog-bk:// should build bookkeeper based distributedlog namespace",
				namespace instanceof BKDistributedLogNamespace);
		} finally {
			namespace.close();
		}
	}
	@Test
	public void testCreateAndOpenDistributedLogSource() throws Exception {
		DistributedLogSource<String> dlsource =
			new DistributedLogSource<>(
			DistributedLogSourceTest.class.getName(),
			Arrays.asList("messaging-stream-1","messaging-stream-2","messaging-stream-3"),
			DistributedLogNamespaceBuilder.newBuilder()
				.uri(new URI("distributedlog://"+zkServers+"/messaging/test_namespace"))
				.conf(new DistributedLogConfiguration()),
			new DistributedLogMemoryOffsetStore(),
			new SimpleStringSchema()
		);
		dlsource.open(new Configuration());
		dlsource.cancel();
		dlsource.close();
	}
}
