package com.gdrivefs.simplecache;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ListIterator;
import java.util.UUID;

class DuplicateRejectingList implements List<File>
{
	List<FileReference> delegate = new ArrayList<FileReference>();

	public DuplicateRejectingList()
	{
	}
	
	public DuplicateRejectingList(Collection<File> seeds)
	{
		addAll(seeds);
	}
	
	@Override
	public boolean add(File element)
	{
		if(contains(element)) throw new Error("Duplicate added: "+element);
		return delegate.add(new FileReference(element));
	}

	@Override
	public void add(int index, File element)
	{
		delegate.add(index, new FileReference(element));
	}

	@Override
	public boolean addAll(Collection<? extends File> c)
	{
		for(File f : c) add(f);
		return c.size() > 0;
	}

	@Override
	public boolean addAll(int index, Collection<? extends File> c)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public void clear()
	{
		delegate.clear();
	}

	@Override
	public boolean contains(Object o)
	{
		if(!(o instanceof File)) return false;
		return delegate.contains(new FileReference((File)o));
	}

	@Override
	public boolean containsAll(Collection<?> c)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public File get(int index)
	{
		try
		{
			return delegate.get(index).get();
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public int indexOf(Object o)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public boolean isEmpty()
	{
		return delegate.isEmpty();
	}

	@Override
	public Iterator<File> iterator()
	{
		final Iterator<FileReference> iterator = delegate.iterator();
		
		return new Iterator<File>()
		{
			@Override
			public void remove()
			{
				iterator.remove();
			}
			
			@Override
			public File next()
			{
				try
				{
					return iterator.next().get();
				}
				catch(IOException e)
				{
					throw new RuntimeException(e);
				}
			}
			
			@Override
			public boolean hasNext()
			{
				return iterator.hasNext();
			}
		};
	}

	@Override
	public int lastIndexOf(Object o)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public ListIterator<File> listIterator()
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public ListIterator<File> listIterator(int index)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public boolean remove(Object o)
	{
		if(!(o instanceof File)) return false;
		return delegate.remove(new FileReference((File)o));
	}

	@Override
	public File remove(int index)
	{
		try
		{
			return delegate.remove(index).get();
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean removeAll(Collection<?> c)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public boolean retainAll(Collection<?> c)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public File set(int index, File element)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public int size()
	{
		return delegate.size();
	}

	@Override
	public List<File> subList(int fromIndex, int toIndex)
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public Object[] toArray()
	{
		Object[] array = new Object[delegate.size()];
		for(int i = 0; i < delegate.size(); i++)
			try
			{
				array[i] = delegate.get(i).get();
			}
			catch(IOException e)
			{
				throw new RuntimeException(e);
			}
		return array;
	}

	@Override
	public <T> T[] toArray(T[] a)
	{
		return (T[])toArray();
	}
	
	
}

class FileReference
{
	Drive drive;
	String googleId;
	UUID internalId;
	SoftReference<File> reference;
	
	public FileReference(Drive drive, String googleId, UUID internalId)
	{
		this.drive = drive;
		this.googleId = googleId;
		this.internalId = internalId;
	}
	
	public FileReference(File file)
	{
		this(file.drive, file.googleFileId, file.localFileId);
		reference = new SoftReference<File>(file);
	}
	
	public File get() throws IOException
	{
		File reference = this.reference.get();
		if(reference != null) return reference;
		reference = drive.getCachedFile(googleId);
		if(reference != null) return reference;
		reference = drive.getFile(internalId);
		return reference;
	}
	
	public boolean equals(Object obj)
	{
		if(!(obj instanceof FileReference)) return false;
		FileReference other = (FileReference)obj;
		
		if(this.googleId != null && other.googleId != null)
			if(this.googleId.equals(other.googleId)) return true;
			else return false;
		if(this.internalId != null && other.internalId != null)
			if(this.internalId.equals(other.internalId)) return true;
			else return false;
		
		try
		{
			return this.get().equals(other.get());
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}
}
