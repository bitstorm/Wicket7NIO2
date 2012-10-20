/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wicket7;

import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FutureChannelWrapper
{

	private AsynchronousFileChannel channel;
	private Future<Integer> futureFile;

	public FutureChannelWrapper(AsynchronousFileChannel channel, Future<Integer> futureFile)
	{
		this.channel = channel;
		this.futureFile = futureFile;
	}

	public AsynchronousFileChannel getChannel()
	{
		return channel;
	}

	public Future<Integer> getFutureFile()
	{
		return futureFile;
	}

	public Integer get() throws InterruptedException, ExecutionException
	{
		return futureFile.get();
	}
}
