/*
	Copyright 2014 ITACA-TSB, http://www.tsb.upv.es
	Instituto Tecnologico de Aplicaciones de Comunicacion
	Avanzadas - Grupo Tecnologias para la Salud y el
	Bienestar (TSB)

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

import org.universAAL.context.che.Hub;
import org.universAAL.context.che.Hub.Log;
import virtuoso.rdf4j.driver.VirtuosoRepository;

/**
 * Extension of {@link org.universAAL.context.che.database.impl.VirtuosoBackend}
 * that uses a modified Virtuoso Store that checks cardinality of stored
 * statements, according to OWL Lite (if maxCardinality is 1 only one single
 * value object is accepted).
 *
 */
public class VirtuosoBackendCrd extends VirtuosoBackend {
	/**
	 * logger.
	 */
	private static Log log = Hub.getLog(VirtuosoBackendCrd.class);

	@Override
	public void connect() {
		log.info("CHe connecting to {} ", VIRTUOSO_JDBC_URL);
		// TODO: Evaluate the inference, and study other reasoners, if any
		try {
			myRepository = new VirtuosoRepository(VIRTUOSO_JDBC_URL,VIRTUOSO_USER,VIRTUOSO_PASSWORD,VIRTUOSO_GRAPH);
			con = myRepository.getConnection();
			if (Boolean.parseBoolean(Hub.getProperties().getProperty("STORE.PRELOAD"))) {
				this.populate();
			}
		} catch (Exception e) {
			log.error("connect", "Exception trying to initilaize the store: {} ", e);
			e.printStackTrace();
		}
	}

}
