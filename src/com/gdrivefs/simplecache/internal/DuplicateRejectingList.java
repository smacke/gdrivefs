package com.gdrivefs.simplecache.internal;

import java.util.ArrayList;
import java.util.Collection;

public class DuplicateRejectingList<E> extends ArrayList<E>
{
	private static final long serialVersionUID = 1L;

	public DuplicateRejectingList()
	{
	}
	
	public DuplicateRejectingList(Collection<E> seeds)
	{
		for(E seed : seeds) add(seed);
	}
	
	@Override
	public boolean add(E element)
	{
		if(contains(element)) throw new Error("Duplicate added: "+element);
		return super.add(element);
	}
}
