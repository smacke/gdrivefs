package com.gdrivefs.simplecache.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

public class DuplicateRejectingList<E> extends ArrayList<E>
{
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
		System.out.println("adding element to list: "+element);
		if(contains(element)) throw new Error("Duplicate added: "+element);
		return super.add(element);
	}
}
