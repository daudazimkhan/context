/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package org.universAAL.context.rdf4j.sail;

import java.io.IOException;

import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.SailReadOnlyException;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.eclipse.rdf4j.sail.helpers.DefaultSailChangedEvent;

/**
 * @author Arjohn Kampman
 * 
 * PATCH UAAL: Copied from NativeStoreConnection and modified to allow encryption 
 * and enforce cardinality 1 of any value on top of previous closed collections
 */
public class Collection2StoreConnection extends SailSourceConnection {

	/*-----------*
	 * Constants *
	 *-----------*/

	protected final Collection2Store nativeStore;

	/*-----------*
	 * Variables *
	 *-----------*/

	protected volatile DefaultSailChangedEvent sailChangedEvent;

	/**
	 * The transaction lock held by this connection during transactions.
	 */
	private volatile Lock txnLock;

	/*--------------*
	 * Constructors *
	 *--------------*/

	protected Collection2StoreConnection(Collection2Store sail)
		throws IOException
	{
		super(sail, sail.getSailStore(), sail.getEvaluationStrategyFactory());
		this.nativeStore = sail;
		sailChangedEvent = new DefaultSailChangedEvent(sail);
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	protected void startTransactionInternal()
		throws SailException
	{
		if (!nativeStore.isWritable()) {
			throw new SailReadOnlyException("Unable to start transaction: data file is locked or read-only");
		}
		boolean releaseLock = true;
		try {
			if (txnLock == null || !txnLock.isActive()) {
				txnLock = nativeStore.getTransactionLock(getTransactionIsolation());
			}
			super.startTransactionInternal();
		}
		finally {
			if (releaseLock && txnLock != null) {
				txnLock.release();
			}
		}
	}

	@Override
	protected void commitInternal()
		throws SailException
	{
		try {
			super.commitInternal();
		}
		finally {
			if (txnLock != null) {
				txnLock.release();
			}
		}

		nativeStore.notifySailChanged(sailChangedEvent);

		// create a fresh event object.
		sailChangedEvent = new DefaultSailChangedEvent(nativeStore);
	}

	@Override
	protected void rollbackInternal()
		throws SailException
	{
		try {
			super.rollbackInternal();
		}
		finally {
			if (txnLock != null) {
				txnLock.release();
			}
		}
		// create a fresh event object.
		sailChangedEvent = new DefaultSailChangedEvent(nativeStore);
	}

	@Override
	protected void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
		throws SailException
	{
		// assume the triple is not yet present in the triple store
		sailChangedEvent.setStatementsAdded(true);
	}

	public boolean addInferredStatement(Resource subj, IRI pred, Value obj, Resource... contexts)
		throws SailException
	{
		boolean ret = super.addInferredStatement(subj, pred, obj, contexts);
		// assume the triple is not yet present in the triple store
		sailChangedEvent.setStatementsAdded(true);
		return ret;
	}

	@Override
	protected void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
		throws SailException
	{
		sailChangedEvent.setStatementsRemoved(true);
	}

	public boolean removeInferredStatement(Resource subj, IRI pred, Value obj, Resource... contexts)
		throws SailException
	{
		boolean ret = super.removeInferredStatement(subj, pred, obj, contexts);
		sailChangedEvent.setStatementsRemoved(true);
		return ret;
	}

	@Override
	protected void clearInternal(Resource... contexts)
		throws SailException
	{
		super.clearInternal(contexts);
		sailChangedEvent.setStatementsRemoved(true);
	}

	public void clearInferred(Resource... contexts)
		throws SailException
	{
		super.clearInferred(contexts);
		sailChangedEvent.setStatementsRemoved(true);
	}
	
	

}
