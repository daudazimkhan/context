/*
	Copyright 2008 ITACA-SABIEN, http://www.sabien.upv.es
	Instituto Tecnologico de Aplicaciones de Comunicacion
	Avanzadas - Grupo Tecnologias para la Salud y el
	Bienestar (SABIEN)

	See the NOTICE file distributed with this work for additional
	information regarding copyright ownership

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	  http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package org.universAAL.context.che.database.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import org.universAAL.context.che.Hub;
import org.universAAL.context.che.Hub.Log;
import org.universAAL.middleware.context.ContextEvent;
import org.universAAL.middleware.rdf.Resource;
import org.universAAL.middleware.util.Constants;

/**
 * Extension of {@link org.universAAL.context.che.database.impl.VirtuosoBackend}
 * that interprets the confidence value of received events before storing them.
 * If the confidence is greater than the threshold passed to this class in the
 * constructor or <code>setThreshold</code>, the event will be stored unchanged
 * as in {@link org.universAAL.context.che.database.impl.VirtuosoBackend}.
 * Otherwise, only statements having the event as subject will be stored, but
 * not reified statements about its subject nor object.
 * <p/>
 * Example:
 * <p/>
 * An "event1" with "subject2" "predicate3" and "object4" with enough confidence
 * will result in having the statements in the store:
 * <p/>
 * <p/>
 * "event1" "hasSubject" "subject2"
 * <p/>
 * "event1" "hasPredicate" "predicate3"
 * <p/>
 * "event1" "hasObject" "object4"
 * <p/>
 * "subject2" "predicate3" "object4"
 * <p/>
 * But if the confidence is below the threshold, the last reified statement is
 * not stored.
 *
 *
 */
public class VirtuosoBackendWithConfidence extends VirtuosoBackend {
	/**
	 * Logger.
	 */
	private static Log log = Hub.getLog(VirtuosoBackendWithConfidence.class);
	/**
	 * Threshold for confidence.
	 */
	private int threshold = 0;

	/**
	 * Main constructor.
	 */
	public VirtuosoBackendWithConfidence() {
		super();
		String conf = Hub.getProperties().getProperty("STORE.CONFIDENCE");
		if (conf != null) {
			try {
				setThreshold(Integer.parseInt(conf));
			} catch (Exception e) {
				log.error("init", "Invalid confidence threshold. Using 0.", e);
				setThreshold(0);
			}

		} else {
			setThreshold(0);
		}
	}

	/**
	 * Constructor with initial confidence.
	 *
	 * @param confidence
	 *            Threshold for confidence
	 */
	public VirtuosoBackendWithConfidence(int confidence) {
		super();
		this.setThreshold(confidence);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.universAAL.context.che.database.impl.VirtuosoBackend#storeEvent(org
	 * .universAAL.middleware.context.ContextEvent)
	 */
	@Override
	synchronized public void storeEvent(ContextEvent e) {
		try {
			try {
				log.debug("storeEvent", "Adding event to store, if enough confidence");
				Integer conf = e.getConfidence();
				// This will stay null if no tenants
				IRI[] contextArray = null;
				if (tenantAware) {
					// Tenant-aware enabled: add tenants as RDF context
					List scopeList = e.getScopes();
					if (!scopeList.isEmpty()) {
						ValueFactory f = myRepository.getValueFactory();
						String[] scopeArray = (String[]) scopeList.toArray(new String[0]);
						contextArray = new IRI[scopeArray.length];
						for (int i = 0; i < scopeArray.length; i++) {
							// Check that scope is valid URI
							contextArray[i] = f.createIRI(Resource.isQualifiedName(scopeArray[i]) ? scopeArray[i]
									: Constants.MIDDLEWARE_LOCAL_ID_PREFIX + scopeArray[i]);
						}
					}
				}
				if (conf != null) {
					if (conf.intValue() < threshold) {
						TurtleParser parser = new TurtleParser();
						StatementCollector stHandler = new StatementCollector();
						parser.setRDFHandler(stHandler);
						parser.parse(new StringReader(serializer.serialize(e)), e.getURI());
						Iterator<Statement> sts = stHandler.getStatements().iterator();
						// store only statements having event as subject
						while (sts.hasNext()) {
							Statement st = sts.next();
							if (st.getSubject().stringValue().equals(e.getURI())) {
								if (contextArray != null) {
									con.add(st, contextArray);
								} else {
									con.add(st);
								}
							}
						}
						log.info("storeEvent", "CHe: Stored a Context Event with low " + "Confidence: Not reified.");
					} else {
						if (contextArray != null) {
							con.add(new StringReader(serializer.serialize(e)), e.getURI(), RDFFormat.TURTLE,
									contextArray);
						} else {
							con.add(new StringReader(serializer.serialize(e)), e.getURI(), RDFFormat.TURTLE);
						}
						log.info("storeEvent", "CHe: Stored a Context Event with high " + "Confidence");
					}
				} else { // TODO: What to do if events have no confidence?
					if (contextArray != null) {
						con.add(new StringReader(serializer.serialize(e)), e.getURI(), RDFFormat.TURTLE, contextArray);
					} else {
						con.add(new StringReader(serializer.serialize(e)), e.getURI(), RDFFormat.TURTLE);
					}
					log.info("storeEvent", "CHe: Stored a Context Event without Confidence");
				}
				log.debug("storeEvent", "Successfully added event to store");
			} catch (IOException exc) {
				log.error("storeEvent",
						"Error trying to add event to the store. " + "In older versions this usually happened "
								+ "because of the underlying connection closing"
								+ " due to inactivity, but now it is because: {}",
						exc);
				exc.printStackTrace();
			}
		} catch (RDF4JException exc) {
			log.error("storeEvent", "Error trying to get connection to store: {}", exc);
			exc.printStackTrace();
		}
	}

	/**
	 * Get the threshold for confidence.
	 *
	 * @return threshold
	 */
	public int getThreshold() {
		return threshold;
	}

	/**
	 * Set the threshold for confidence.
	 *
	 * @param threshold
	 *            for confidence
	 */
	public final void setThreshold(int threshold) {
		if (threshold < 100) {
			this.threshold = threshold;
		} else {
			this.threshold = 100;
		}

	}
}
