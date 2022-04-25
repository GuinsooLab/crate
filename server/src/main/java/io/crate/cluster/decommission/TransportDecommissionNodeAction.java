/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.cluster.decommission;

import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CompletableFuture;

@Singleton
public class TransportDecommissionNodeAction extends TransportAction<DecommissionNodeRequest, AcknowledgedResponse>
    implements NodeAction<DecommissionNodeRequest, AcknowledgedResponse> {

    private static final String EXECUTOR = ThreadPool.Names.MANAGEMENT;

    private final DecommissioningService decommissioningService;
    private final Transports transports;

    @Inject
    public TransportDecommissionNodeAction(TransportService transportService,
                                           DecommissioningService decommissioningService,
                                           Transports transports) {
        super(DecommissionNodeAction.NAME);
        this.decommissioningService = decommissioningService;
        this.transports = transports;
        transportService.registerRequestHandler(
            DecommissionNodeAction.NAME,
            EXECUTOR,
            DecommissionNodeRequest::new,
            new NodeActionRequestHandler<>(this)
        );
    }

    @Override
    public void doExecute(DecommissionNodeRequest request,
                             ActionListener<AcknowledgedResponse> listener) {
        transports.sendRequest(
            DecommissionNodeAction.NAME,
            request.nodeId(),
            request,
            listener,
            new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::new)
        );
    }

    @Override
    public CompletableFuture<AcknowledgedResponse> nodeOperation(DecommissionNodeRequest request) {
        try {
            return decommissioningService.decommission().thenApply(aVoid -> new AcknowledgedResponse(true));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }
}
