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
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.distributedlog.internal.DistributedLogOffsetStore;
import org.apache.flink.streaming.connectors.distributedlog.internal.DistributedLogStreamReader;
import org.apache.flink.streaming.connectors.distributedlog.internal.impl.DistributedLogStreamReaderImpl;
import org.apache.flink.streaming.connectors.distributedlog.model.DistributedLogStream;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@PublicEvolving
public class DistributedLogSource<T> extends RichParallelSourceFunction<T> implements
	CheckpointListener,
	CheckpointedAsynchronously<HashMap<DistributedLogStream, DLSN>>,
	ResultTypeQueryable<T> {

	private static final Logger LOG = LoggerFactory.getLogger(DistributedLogSource.class);

	/**
	 * The maximum number of pending non-committed checkpoints to track, to avoid memory leaks
	 */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

	// ------------------------------------------------------------------------
	//  configuration state, set on the client relevant for all subtasks
	// ------------------------------------------------------------------------

	/**
	 * All subscribed streams.
	 */
	private final List<DistributedLogStream> subscribedStreams;

	private final String clientId;
	private final DistributedLogNamespaceBuilder namespace;
	private final DistributedLogOffsetStore offsetStore;
	private final DeserializationSchema<T> deserializer;

	// ------------------------------------------------------------------------
	//  runtime state (used individually by each parallel subtask)
	// ------------------------------------------------------------------------

	/**
	 * Data for pending but uncommitted sequential checkpoints
	 */
	private final LinkedMap pendingCheckpoints = new LinkedMap();

	/**
	 * Streams streamReader for assigned streams in current sub-task
	 */
	private transient volatile DistributedLogStreamReader<T> streamReader;

	/**
	 * The offset snapshot to restore from certain checkpoint
	 */
	private transient volatile HashMap<DistributedLogStream, DLSN> restoredDLSNOffsets;

	/**
	 * Flag indicating whether streamReader is still running
	 */
	private volatile boolean running = true;

	// ------------------------------------------------------------------------
	//  Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new Flink DistributedLog Source.
	 */
	public DistributedLogSource(String clientId,
								List<String> streamNames,
								DistributedLogNamespaceBuilder namespace,
								DistributedLogOffsetStore offsetStore,
								DeserializationSchema<T> deserializer) {
		Preconditions.checkNotNull(clientId,"clientId");
		Preconditions.checkNotNull(streamNames,"streamNames");
		Preconditions.checkArgument(streamNames.size()>0,"streamNames' length should be greater than 0");
		Preconditions.checkNotNull(namespace,"namespace");
		Preconditions.checkNotNull(offsetStore,"offsetStore");
		Preconditions.checkNotNull(deserializer,"deserializer");

		this.clientId = clientId;
		this.namespace = namespace;
		this.offsetStore = offsetStore;
		this.deserializer = deserializer;

		List<DistributedLogStream> distributedLogStreams = new ArrayList<>(streamNames.size());
		for (String streamName : streamNames) {
			DistributedLogStream distributedLogStream = new DistributedLogStream(streamName);
			distributedLogStreams.add(distributedLogStream);
		}

		this.subscribedStreams = Collections.unmodifiableList(distributedLogStreams);

		if (LOG.isInfoEnabled()) {
			StringBuilder sb = new StringBuilder();
			for (DistributedLogStream stream : this.subscribedStreams) {
				sb.append(stream.getStreamName()).append(", ");
			}
			LOG.info("Flink DistributedLog Source is going to subscribe from following streams: {}", sb.toString());
		}
	}


	// ------------------------------------------------------------------------
	//  Source life cycle
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		// restore to last DLSN offset from latest complete snapshot
		if (restoredDLSNOffsets != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Subtask {} is restoring DLSN offsets {} from previous checking pointed state",
					getRuntimeContext().getIndexOfThisSubtask(), this.restoredDLSNOffsets.toString());
			}
		}
	}

	/**
	 * Snapshot
	 *
	 * @param checkpointId        The ID of the checkpoint.
	 * @param checkpointTimestamp The timestamp of the checkpoint, as derived by
	 *                            System.currentTimeMillis() on the JobManager.
	 * @return
	 * @throws Exception
	 */
	@Override
	public HashMap<DistributedLogStream, DLSN> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
			return null;
		}
		final DistributedLogStreamReader<T> reader = this.streamReader;
		if (reader == null) {
			// readers has not been initialized, return original restored DLSN offsets.
			return restoredDLSNOffsets;
		}

		HashMap<DistributedLogStream, DLSN> currentOffsets = reader.snapshotDLSNOffsets();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state. DLSN offsets: {}, checkpoint id: {}, timestamp: {}",
				currentOffsets.toString(), checkpointId, checkpointTimestamp);
		}

		// the map cannot be asynchronously updated, because only one checkpoint call can happen
		// on this function at a time: either snapshotState() or notifyCheckpointComplete()
		pendingCheckpoints.put(checkpointId, currentOffsets);

		// truncate the map, to prevent infinite growth
		while (pendingCheckpoints.size() > MAX_NUM_PENDING_CHECKPOINTS) {
			pendingCheckpoints.remove(0);
		}

		return currentOffsets;
	}

	@Override
	public void restoreState(HashMap<DistributedLogStream, DLSN> restoredDLSNOffsets) throws Exception {
		LOG.info("Setting restore DLSN offsets state: {}", restoredDLSNOffsets);
		this.restoredDLSNOffsets = restoredDLSNOffsets;
	}

	/**
	 * Snapshot offset to certain checkpoint after checkpoint is completed.
	 *
	 * @param checkpointId The ID of the checkpoint that has been completed.
	 * @throws Exception
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (!running) {
			LOG.debug("notifyCheckpointComplete() called on closed source");
			return;
		}

		final DistributedLogStreamReader<T> reader = this.streamReader;
		if (reader == null) {
			LOG.debug("notifyCheckpointComplete() called on uninitialized source");
			return;
		}

		// only one commit operation must be in progress
		if (LOG.isDebugEnabled()) {
			LOG.debug("Committing DLSN offsets for checkpoint {}", checkpointId);
		}

		try {
			final int posInMap = pendingCheckpoints.indexOf(checkpointId);

			if (posInMap == -1) {
				LOG.warn("Received confirmation from unknown checkpoint id {}", checkpointId);
				return;
			}

			@SuppressWarnings("unchecked")
			HashMap<DistributedLogStream, DLSN> completedCheckpointOffsets =
				(HashMap<DistributedLogStream, DLSN>) pendingCheckpoints.remove(posInMap);

			// give up older checkpoints
			for (int i = 0; i < posInMap; i++) {
				pendingCheckpoints.remove(0);
			}

			if (completedCheckpointOffsets == null || completedCheckpointOffsets.size() == 0) {
				LOG.debug("Checkpoint state was empty.");
				return;
			}
			reader.commitDLSNOffsets(completedCheckpointOffsets);
		} catch (Exception e) {
			if (running) {
				throw e;
			}
			// else ignore exception if the sub task is no longer running
		}
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return this.deserializer.getProducedType();
	}

	// ------------------------------------------------------------------------
	//  Work methods
	// ------------------------------------------------------------------------

	/**
	 * Fetch data from assigned streams.
	 */
	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		// figure out which streams this subtask should process
		final List<DistributedLogStream> assignedStreamsForThisTask = assignStreamsForTask(this.subscribedStreams,
			getRuntimeContext().getNumberOfParallelSubtasks(), getRuntimeContext().getIndexOfThisSubtask());

		// If streams assigned
		if (!assignedStreamsForThisTask.isEmpty()) {
			// (1) create streamReader that communicate with DistributedLog Service
			final DistributedLogStreamReader<T> reader = createReader(this.namespace.build(),
				this.offsetStore, assignedStreamsForThisTask, sourceContext, getRuntimeContext());

			// (2) set the streamReader to the restored checkpoint DLSN offsets.
			if (restoredDLSNOffsets != null) {
				reader.restoreDLSNOffsets(restoredDLSNOffsets);
			}

			// publish the streamReader reference for snapshot-, commit- and cancel calls.
			this.streamReader = reader;

			if (!running) {
				return;
			}

			// (3) run the streamReader's main work method
			reader.runReadLoop();
		} else {
			// TODO: this source never completes, so emit a Long.MAX_VALUE watermark to avoid blocking watermark forwarding
			// sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));

			// wait until this is canceled
			final Object waitLock = new Object();
			while (running) {
				try {
					//noinspection SynchronizationOnLocalVariableOrMethodParameter
					synchronized (waitLock) {
						waitLock.wait();
					}
				} catch (InterruptedException e) {
					if (!running) {
						// restore the interrupted state, and fall through the loop
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	}

	@Override
	public void cancel() {
		// shutdown worker.
		running = false;

		// abort the streamReader, if there is one.
		if (streamReader != null) {
			streamReader.cancel();
		}
	}

	@Override
	public void close() throws Exception {
		try {
			cancel();
		} finally {
			super.close();
		}
	}

	// ------------------------------------------------------------------------
	//  DistributedLog Reader specific methods
	// ------------------------------------------------------------------------

	private DistributedLogStreamReader<T> createReader(DistributedLogNamespace namespace, DistributedLogOffsetStore offsetStore, List<DistributedLogStream> assignedStreams, SourceContext<T> sourceContext, RuntimeContext runtimeContext) throws IOException {
		return new DistributedLogStreamReaderImpl<>(namespace, offsetStore, assignedStreams, deserializer, sourceContext, runtimeContext);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Selects which of given streams should be handled by a specific task,
	 * given a certain number of tasks.
	 *
	 * @param subscribedStreams        The streams to select from
	 * @param numberOfParallelSubtasks The number of tasks
	 * @param indexOfThisSubtask       The index of the specific task
	 * @return The sublist of streams to be handled by that task.
	 */
	private static List<DistributedLogStream> assignStreamsForTask(List<DistributedLogStream> subscribedStreams, int numberOfParallelSubtasks, int indexOfThisSubtask) {
		final List<DistributedLogStream> thisSubtaskPartitions = new ArrayList<>(
			subscribedStreams.size() / numberOfParallelSubtasks + 1);

		for (int i = 0; i < subscribedStreams.size(); i++) {
			if (i % numberOfParallelSubtasks == indexOfThisSubtask) {
				thisSubtaskPartitions.add(subscribedStreams.get(i));
			}
		}

		return thisSubtaskPartitions;
	}
}
