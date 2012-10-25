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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.wicket.WicketRuntimeException;
import org.apache.wicket.pageStore.IDataStore;
import org.apache.wicket.pageStore.PageWindowManager;
import org.apache.wicket.pageStore.PageWindowManager.PageWindow;
import org.apache.wicket.util.io.IOUtils;
import org.apache.wicket.util.lang.Args;
import org.apache.wicket.util.lang.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A data store implementation which stores the data on disk (in a file system)
 */
public class AsynchDiskDataStore implements IDataStore
{
	private static final Logger log = LoggerFactory.getLogger(AsynchDiskDataStore.class);

	private static final String INDEX_FILE_NAME = "DiskDataStoreIndex";

	private final String applicationName;

	private final Bytes maxSizePerPageSession;

	private final File fileStoreFolder;

	private final ConcurrentMap<String, SessionEntry> sessionEntryMap;

	/**
	 * Construct.
	 * 
	 * @param applicationName
	 * @param fileStoreFolder
	 * @param maxSizePerSession
	 */
	public AsynchDiskDataStore(final String applicationName, final File fileStoreFolder,
		final Bytes maxSizePerSession)
	{
		this.applicationName = applicationName;
		this.fileStoreFolder = fileStoreFolder;
		maxSizePerPageSession = Args.notNull(maxSizePerSession, "maxSizePerSession");
		sessionEntryMap = new ConcurrentHashMap<String, SessionEntry>();

		try
		{
			if (this.fileStoreFolder.exists() || this.fileStoreFolder.mkdirs())
			{
				loadIndex();
			}
			else
			{
				log.warn("Cannot create file store folder for some reason.");
			}
		}
		catch (SecurityException e)
		{
			throw new WicketRuntimeException(
				"SecurityException occurred while creating DiskDataStore. Consider using a non-disk based IDataStore implementation. "
					+ "See org.apache.wicket.Application.setPageManagerProvider(IPageManagerProvider)",
				e);
		}
	}

	/**
	 * @see org.apache.wicket.pageStore.IDataStore#destroy()
	 */
	@Override
	public void destroy()
	{
		log.debug("Destroying...");
		saveIndex();
		log.debug("Destroyed.");
	}

	/**
	 * @see org.apache.wicket.pageStore.IDataStore#getData(java.lang.String, int)
	 */
	@Override
	public byte[] getData(final String sessionId, final int id)
	{
		byte[] pageData = null;
		SessionEntry sessionEntry = getSessionEntry(sessionId, false);
		if (sessionEntry != null)
		{
			pageData = sessionEntry.loadPage(id);
		}

		log.debug("Returning data{} for page with id '{}' in session with id '{}'", new Object[] {
				pageData != null ? "" : "(null)", id, sessionId });
		return pageData;
	}

	/**
	 * @see org.apache.wicket.pageStore.IDataStore#isReplicated()
	 */
	@Override
	public boolean isReplicated()
	{
		return false;
	}

	/**
	 * @see org.apache.wicket.pageStore.IDataStore#removeData(java.lang.String, int)
	 */
	@Override
	public void removeData(final String sessionId, final int id)
	{
		SessionEntry sessionEntry = getSessionEntry(sessionId, false);
		if (sessionEntry != null)
		{
			log.debug("Removing data for page with id '{}' in session with id '{}'", new Object[] {
					id, sessionId });
			sessionEntry.removePage(id);
		}
	}

	/**
	 * @see org.apache.wicket.pageStore.IDataStore#removeData(java.lang.String)
	 */
	@Override
	public void removeData(final String sessionId)
	{
		SessionEntry sessionEntry = getSessionEntry(sessionId, false);
		if (sessionEntry != null)
		{
			log.debug("Removing data for pages in session with id '{}'", sessionId);
			synchronized (sessionEntry)
			{
				sessionEntryMap.remove(sessionEntry.sessionId);
				sessionEntry.unbind();
			}
		}
	}

	/**
	 * @see org.apache.wicket.pageStore.IDataStore#storeData(java.lang.String, int, byte[])
	 */
	@Override
	public void storeData(final String sessionId, final int id, final byte[] data)
	{
		SessionEntry sessionEntry = getSessionEntry(sessionId, true);
		if (sessionEntry != null)
		{
			log.debug("Storing data for page with id '{}' in session with id '{}'", new Object[] {
					id, sessionId });
			sessionEntry.savePage(id, data);
		}
	}

	/**
	 * 
	 * @param sessionId
	 * @param create
	 * @return the session entry
	 */
	protected SessionEntry getSessionEntry(final String sessionId, final boolean create)
	{
		if (!create)
		{
			return sessionEntryMap.get(sessionId);
		}

		SessionEntry entry = new SessionEntry(this, sessionId);
		SessionEntry existing = sessionEntryMap.putIfAbsent(sessionId, entry);
		return existing != null ? existing : entry;
	}

	/**
	 * Load the index
	 */
	@SuppressWarnings("unchecked")
	private void loadIndex()
	{
		Path storeFolder = getStoreFolder();
		Path indexPath = storeFolder.resolve(INDEX_FILE_NAME);		
		File index = indexPath.toFile();
		
		if (Files.exists(indexPath) && index.length() > 0)
		{
			try
			{
				InputStream stream = new FileInputStream(index);
				ObjectInputStream ois = new ObjectInputStream(stream);
				try
				{
					Map<String, SessionEntry> map = (Map<String, SessionEntry>)ois.readObject();
					sessionEntryMap.clear();
					sessionEntryMap.putAll(map);

					for (Entry<String, SessionEntry> entry : sessionEntryMap.entrySet())
					{
						// initialize the diskPageStore reference
						SessionEntry sessionEntry = entry.getValue();
						sessionEntry.diskDataStore = this;
					}
				}
				finally
				{
					stream.close();
					ois.close();
				}
			}
			catch (Exception e)
			{
				log.error("Couldn't load DiskDataStore index from file " + indexPath + ".", e);
			}
		}
		try {			
			Files.deleteIfExists(indexPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean isEmpty(Path path) {
		DirectoryStream<Path> dirStream = null;
		boolean isEmpty = true;
		
		try {
			dirStream = Files.newDirectoryStream(path);
		
			Iterator<Path> dirIterator = dirStream.iterator();
			isEmpty = !dirIterator.hasNext();
			dirStream.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return isEmpty;
	}
	
	public static void deleteFolder(Path path){
		File dirFile = path.toFile();
		if(!dirFile.isDirectory())
			return;
		
		org.apache.wicket.util.file.Files.removeFolder(dirFile);
	}

	/**
	 * 
	 */
	private void saveIndex()
	{
		Path storeFolder = getStoreFolder();
		if (Files.exists(storeFolder))
		{
			Path index = storeFolder.resolve(INDEX_FILE_NAME);
			try
			{
				Files.deleteIfExists(index);
				OutputStream stream = new FileOutputStream(index.toFile());
				ObjectOutputStream oos = new ObjectOutputStream(stream);
				try
				{
					Map<String, SessionEntry> map = new HashMap<String, SessionEntry>(
						sessionEntryMap.size());
					for (Entry<String, SessionEntry> e : sessionEntryMap.entrySet())
					{
						if (e.getValue().unbound == false)
						{
							map.put(e.getKey(), e.getValue());
						}
					}
					oos.writeObject(map);
				}
				finally
				{
					stream.close();
					oos.close();
				}
			}
			catch (Exception e)
			{
				log.error("Couldn't write DiskDataStore index to file " + index + ".", e);
			}
		}
	}

	/**
	 * 
	 */
	protected static class SessionEntry implements Serializable
	{
		private static final long serialVersionUID = 1L;

		private final String sessionId;
		private transient AsynchDiskDataStore diskDataStore;
		private Path fileName;
		private PageWindowManager manager;
		private boolean unbound = false;

		private final transient ConcurrentHashMap<Integer, Semaphore> semaphoreFiles = new ConcurrentHashMap<Integer, Semaphore>();

		protected SessionEntry(AsynchDiskDataStore diskDataStore, String sessionId)
		{
			this.diskDataStore = diskDataStore;
			this.sessionId = sessionId;
		}

		public PageWindowManager getManager()
		{
			if (manager == null)
			{
				manager = new PageWindowManager(diskDataStore.maxSizePerPageSession.bytes());
			}
			return manager;
		}

		private Path getFileName()
		{
			if (fileName == null)
			{
				fileName = diskDataStore.getSessionFileName(sessionId, true);
			}
			return fileName;
		}

		/**
		 * @return session id
		 */
		public String getSessionId()
		{
			return sessionId;
		}

		/**
		 * Saves the serialized page to appropriate file.
		 * 
		 * @param pageId
		 * @param data
		 */
		public synchronized void savePage(int pageId, byte data[])
		{
			if (unbound)
			{
				return;
			}
			// only save page that has some data
			if (data != null)
			{
				// allocate window for page
				PageWindow window = getManager().createPageWindow(pageId, data.length);

				final AsynchronousFileChannel channel = getFileChannel(true);
				if (channel != null)
				{
					Semaphore semaphore = new Semaphore(0);

					channel.write(ByteBuffer.wrap(data), window.getFilePartOffset(), semaphore, new ChannelCompletionHandler(channel));
					semaphoreFiles.put(pageId, semaphore);
				}
				else
				{
					log.warn(
						"Cannot save page with id '{}' because the data file cannot be opened.",
						pageId);
				}
			}
		}

		/**
		 * Removes the page from pagemap file.
		 * 
		 * @param pageId
		 */
		public synchronized void removePage(int pageId)
		{
			if (unbound)
			{
				return;
			}
			getManager().removePage(pageId);
		}

		/**
		 * Loads the part of pagemap file specified by the given PageWindow.
		 * 
		 * @param window
		 * @return serialized page data
		 */
		public byte[] loadPage(PageWindow window)
		{
			byte[] result = null;

			checkFileReady(window.getPageId());

			AsynchronousFileChannel channel = getFileChannel(false);
			if (channel != null)
			{
				ByteBuffer buffer = ByteBuffer.allocate(window.getFilePartSize());
				try
				{
					Future<Integer> fileLoading = channel.read(buffer, window.getFilePartOffset());
					fileLoading.get();
					if (buffer.hasArray())
					{
						result = buffer.array();
					}
				}
				catch (InterruptedException e)
				{
					throw new RuntimeException(e);
				}
				catch (ExecutionException e)
				{
					throw new RuntimeException(e);
				}
				finally
				{
					IOUtils.closeQuietly(channel);
				}
			}
			return result;
		}

		private void checkFileReady(int pageId)
		{
			Semaphore semaphoreFile = semaphoreFiles.get(pageId);
			
			if(semaphoreFile == null) return;
			
			try
			{
				semaphoreFile.acquire();
				semaphoreFiles.remove(pageId);
			}
			catch (InterruptedException e)
			{
				throw new RuntimeException(e);
			}			
		}

		private AsynchronousFileChannel getFileChannel(boolean create)
		{
			AsynchronousFileChannel channel = null;
			Path file = getFileName();
			if (create || java.nio.file.Files.exists(file))
			{
				
				try
				{
					List<OpenOption> options = new ArrayList<OpenOption>();
					options.add(StandardOpenOption.READ);

					if (create)
					{
						options.add(StandardOpenOption.WRITE);
						options.add(StandardOpenOption.CREATE);
					}
					channel = AsynchronousFileChannel.open(file, options.toArray(new OpenOption[]{}));

				}
				catch (IOException e)
				{
					log.error(e.getMessage(), e);
				}
			}
			return channel;
		}

		/**
		 * Loads the specified page data.
		 * 
		 * @param id
		 * @return page data or null if the page is no longer in pagemap file
		 */
		public synchronized byte[] loadPage(int id)
		{
			if (unbound)
			{
				return null;
			}
			byte[] result = null;
			PageWindow window = getManager().getPageWindow(id);
			if (window != null)
			{
				result = loadPage(window);
			}
			return result;
		}

		/**
		 * Deletes all files for this session.
		 */
		public synchronized void unbind()
		{
			Path sessionFolder = diskDataStore.getSessionFolder(sessionId, false);
			if (Files.exists(sessionFolder))
			{
				deleteFolder(sessionFolder);
				cleanup(sessionFolder);
			}
			unbound = true;
		}

		/**
		 * deletes the sessionFolder's parent and grandparent, if (and only if) they are empty.
		 * 
		 * @see #createPathFrom(String sessionId)
		 * @param sessionFolder
		 *            must not be null
		 */
		private void cleanup(final Path sessionFolder)
		{
			Path high = sessionFolder.getParent();
			try{
				
				if (isEmpty(high))
				{
					Files.delete(high);
					Path low = high.getParent();
					
					if (isEmpty(low))
						Files.delete(low);					
				}
								
			}catch(IOException e){
				e.printStackTrace();
			}						
		}
	}

	/**
	 * Returns the file name for specified session. If the session folder (folder that contains the
	 * file) does not exist and createSessionFolder is true, the folder will be created.
	 * 
	 * @param sessionId
	 * @param createSessionFolder
	 * @return file name for pagemap
	 */
	private Path getSessionFileName(String sessionId, boolean createSessionFolder)
	{
		Path sessionFolder = getSessionFolder(sessionId, createSessionFolder);
		return sessionFolder.resolve("data");
	}

	/**
	 * This folder contains sub-folders named as the session id for which they hold the data.
	 * 
	 * @return the folder where the pages are stored
	 */
	protected Path getStoreFolder()
	{
		Path storeFolderPath = null;
		storeFolderPath = Paths.get(fileStoreFolder.getAbsolutePath(), applicationName + "-filestore");
		return storeFolderPath;
	}

	/**
	 * Returns the folder for the specified sessions. If the folder doesn't exist and the create
	 * flag is set, the folder will be created.
	 * 
	 * @param sessionId
	 * @param create
	 * @return folder used to store session data
	 */
	protected Path getSessionFolder(String sessionId, final boolean create)
	{
		Path storeFolder = getStoreFolder();

		sessionId = sessionId.replace('*', '_');
		sessionId = sessionId.replace('/', '_');
		sessionId = sessionId.replace(':', '_');

		sessionId = createPathFrom(sessionId);

		Path sessionFolder = storeFolder.resolve(sessionId);
		if (create && !java.nio.file.Files.exists(sessionFolder))
		{
			try {
				java.nio.file.Files.createDirectories(sessionFolder);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sessionFolder;
	}

	/**
	 * creates a three-level path from the sessionId in the format 0000/0000/<sessionId>. The two
	 * prefixing directories are created from the sesionId's hascode and thus, should be well
	 * distributed.
	 * 
	 * This is used to avoid problems with Filesystems allowing no more than 32k entries in a
	 * directory.
	 * 
	 * Note that the prefix paths are created from Integers and not guaranteed to be four chars
	 * long.
	 * 
	 * @param sessionId
	 *            must not be null
	 * @return path in the form 0000/0000/sessionId
	 */
	private String createPathFrom(final String sessionId)
	{
		int hash = Math.abs(sessionId.hashCode());
		String low = String.valueOf(hash % 9973);
		String high = String.valueOf((hash / 9973) % 9973);
		StringBuilder bs = new StringBuilder(sessionId.length() + 10);
		bs.append(low);
		bs.append(File.separator);
		bs.append(high);
		bs.append(File.separator);
		bs.append(sessionId);

		return bs.toString();
	}

	@Override
	public boolean canBeAsynchronous()
	{
		return true;
	}
}
