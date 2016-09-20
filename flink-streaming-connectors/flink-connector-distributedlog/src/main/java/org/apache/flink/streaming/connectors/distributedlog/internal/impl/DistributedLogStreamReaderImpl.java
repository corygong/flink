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

package org.apache.flink.streaming.connectors.distributedlog.internal.impl;

import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.distributedlog.internal.DistributedLogStreamState;
import org.apache.flink.streaming.connectors.distributedlog.internal.DistributedLogOffsetStore;
import org.apache.flink.streaming.connectors.distributedlog.internal.DistributedLogStreamReader;
import org.apache.flink.streaming.connectors.distributedlog.model.DistributedLogStream;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DistributedLogStreamReaderImpl<T> implements DistributedLogStreamReader<T>, Runnable {
	private final static Logger LOG = LoggerFactory.getLogger(DistributedLogStreamReaderImpl.class);

	private final SourceFunction.SourceContext<T> sourceContext;
	private final Object checkpointLock;
	private final DistributedLogStreamState[] streamStates;
	private final DistributedLogOffsetStore offsetStore;
	private final RuntimeContext runtimeContext;
	private final DistributedLogNamespace namespace;
	private final DeserializationSchema<T> deserializer;

	/** Flag to mark the main work loop as alive */
	private volatile boolean running = true;

	public DistributedLogStreamReaderImpl(
		DistributedLogNamespace namespace,
		DistributedLogOffsetStore offsetStore,
		List<DistributedLogStream> assignedStreams,
		DeserializationSchema<T> deserializer,
		SourceFunction.SourceContext<T> sourceContext,
		RuntimeContext runtimeContext){
		this.namespace = namespace;
		this.sourceContext = checkNotNull(sourceContext);
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.streamStates = initializeStreamsStates(assignedStreams);
		this.offsetStore = offsetStore;
		this.runtimeContext = runtimeContext;
		this.deserializer = deserializer;

		offsetStore.open();
	}

	private DistributedLogStreamState[] initializeStreamsStates(
		List<DistributedLogStream> assignedStreams){
		DistributedLogStreamState[] streamStates =
			new DistributedLogStreamState[assignedStreams.size()];

		int pos = 0;
		for(DistributedLogStream stream: assignedStreams) {
			streamStates[pos++] = new DistributedLogStreamState(stream);
		}
		return streamStates;
	}

	@Override
	public void runReadLoop() {
		Thread runner = new Thread(this,"DistributedLogStreamReader for "+ this.runtimeContext.getTaskNameWithSubtasks());
		runner.setDaemon(true);
		runner.start();

		try {
			runner.join();
		} catch (InterruptedException e){
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void run() {
		DistributedLogManager[] managers = new DistributedLogManager[streamStates.length];
		AsyncLogReader[] readers = new AsyncLogReader[streamStates.length];
		try {
			int pos = 0;
			for (DistributedLogStreamState streamState : streamStates) {
				LOG.info("Opening log stream {}",streamState.getStream().getStreamName());
				managers[pos++] = namespace.openLog(streamState.getStream().getStreamName());
			}

			final CountDownLatch keepAliveLatch = new CountDownLatch(1);

			pos = 0;

			for(DistributedLogManager dlm: managers){
				final DistributedLogStreamState state = streamStates[pos];
				DLSN offset = state.getOffset();
				if(offset == null){
					offset = DLSN.InitialDLSN;
				}
				AsyncLogReader reader;
				try {
					reader = dlm.getAsyncLogReader(offset);
					reader.readNext().addEventListener(new StreamEventListener<>(reader, deserializer, dlm, state, keepAliveLatch));
				} catch (IOException e) {
					LOG.error("Failed to open async log reader of log stream {} from {}",dlm.getStreamName(),offset);
					throw new IOException(e.getCause());
				}
				readers[pos] = reader;
				pos ++;
			}
			keepAliveLatch.await();
		} catch (Throwable t){
			if(running) {
				LOG.error("Got exception, stop ReaderThread",t);
				running = false;
			} else {
				LOG.debug("Stopped ReaderThread threw exception",t);
			}
		} finally {
			for(AsyncLogReader reader: readers){
				if(reader != null) {
					try {
						FutureUtils.result(reader.asyncClose(), Duration.apply(5, TimeUnit.SECONDS));
					} catch (Throwable e) {
						LOG.error("Got error to close log stream async reader of {}",reader.getStreamName());
					}
				}
			}
			for(DistributedLogManager dlm: managers){
				if(dlm!=null) {
					try {
						dlm.close();
					} catch (Throwable e) {
						LOG.error("Got error to close log stream {}", dlm.getStreamName());
					}
				}
			}
			this.namespace.close();
			this.offsetStore.close();
		}
	}

	private class StreamEventListener<T> implements FutureEventListener<LogRecordWithDLSN> {
		private final CountDownLatch keepAliveLatch;
		private final DistributedLogManager manager;
		private final AsyncLogReader reader;
		private final DistributedLogStreamState state;
		private final DeserializationSchema<T> deserializer;

		public StreamEventListener(AsyncLogReader reader,DeserializationSchema<T> deserializer, DistributedLogManager manager, DistributedLogStreamState state,CountDownLatch keepAliveLatch){
			this.reader = reader;
			this.deserializer = deserializer;
			this.keepAliveLatch = keepAliveLatch;
			this.manager = manager;
			this.state = state;
		}

		@Override
		public void onFailure(Throwable cause) {
			if (cause instanceof LogNotFoundException) {
				LOG.error("Log stream " + manager.getStreamName() + " is not found. Please create it first.",cause);
				keepAliveLatch.countDown();
			} else if (cause instanceof LogEmptyException) {
				LOG.warn("Log stream " + manager.getStreamName() + " is empty.",cause);
				commitAndReadNext(DLSN.InitialDLSN);
			} else {
				LOG.error("Encountered exception on process stream " + manager.getStreamName(),cause);
				keepAliveLatch.countDown();
			}
		}

		@Override
		public void onSuccess(LogRecordWithDLSN record) {
			try {
				// TODO:
				LOG.info("Read record: {}",record);
				deserializer.deserialize(record.getPayload());
			} catch (IOException e) {
				LOG.error("Failed to deserialize record {}",record,e);
			}
			commitAndReadNext(record.getDlsn());
		}

		private void commitAndReadNext(DLSN dlsn){
			state.setOffset(dlsn);
			if(running) {
				reader.readNext().addEventListener(this);
			} else {
				keepAliveLatch.countDown();
			}
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}

	/**
	 * Takes a snapshot of stream offsets.
	 *
	 * @return A map from stream to current sub offset.
     */
	@Override
	public HashMap<DistributedLogStream, DLSN> snapshotDLSNOffsets() {
		assert Thread.holdsLock(checkpointLock);

		HashMap<DistributedLogStream, DLSN> state = new HashMap<>(streamStates.length);
		for(DistributedLogStreamState streamState: streamStates){
			if(streamState.isOffsetDefined()){
				state.put(streamState.getStream(),streamState.getOffset());
			}
		}
		return state;
	}

	@Override
	public void restoreDLSNOffsets(HashMap<DistributedLogStream, DLSN> snapshot) throws Exception {
		for(DistributedLogStreamState streamState: streamStates){
			DLSN offset = snapshot.get(streamState.getStream());
			if(offset != null) {
				streamState.setOffset(offset);
			}
		}
	}

	@Override
	public void commitDLSNOffsets(HashMap<DistributedLogStream,DLSN> offset) {
		for(Map.Entry<DistributedLogStream,DLSN> entry: offset.entrySet()){
			this.offsetStore.commitDLSNOffset(entry.getKey(),entry.getValue());
		}
	}
}
