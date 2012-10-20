package org.wicket7;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Semaphore;

public class ChannelCompletionHandler implements CompletionHandler {
	private Closeable channel;
	
	public ChannelCompletionHandler(Closeable channel) {		
		this.channel = channel;
	}

	@Override
	public void completed(Object result, Object attachment) {
		Semaphore semaphore = (Semaphore) attachment;
		semaphore.release();
		
		try {
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void failed(Throwable exc, Object attachment) {
		Semaphore semaphore = (Semaphore) attachment;
		semaphore.release();
		
		try {
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
