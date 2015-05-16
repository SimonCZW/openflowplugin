/**
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.openflowplugin.impl.services;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.Future;
import org.opendaylight.openflowjava.protocol.api.connection.OutboundQueue;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceContext;
import org.opendaylight.openflowplugin.api.openflow.device.RequestContext;
import org.opendaylight.openflowplugin.api.openflow.device.RequestContextStack;
import org.opendaylight.openflowplugin.api.openflow.device.Xid;
import org.opendaylight.openflowplugin.api.openflow.statistics.ofpspecific.MessageSpy;
import org.opendaylight.openflowplugin.openflow.md.core.sal.convertor.PortConvertor;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.port.rev130925.port.mod.port.Port;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.FlowModInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.OfHeader;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.PortModInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.PortModInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.port.service.rev131107.SalPortService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.port.service.rev131107.UpdatePortInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.port.service.rev131107.UpdatePortOutput;
import org.opendaylight.yangtools.yang.common.RpcError;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;

public class SalPortServiceImpl extends CommonService implements SalPortService {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(SalPortServiceImpl.class);

    public SalPortServiceImpl(final RequestContextStack requestContextStack, final DeviceContext deviceContext) {
        super(requestContextStack, deviceContext);
    }

    @Override
    public Future<RpcResult<UpdatePortOutput>> updatePort(final UpdatePortInput input) {
        return this.<UpdatePortOutput, Void>handleServiceCall(new Function<RequestContext<UpdatePortOutput>, ListenableFuture<RpcResult<Void>>>() {
            @Override
            public ListenableFuture<RpcResult<Void>> apply(final RequestContext<UpdatePortOutput> requestContext) {
                getMessageSpy().spyMessage(input.getImplementedInterface(), MessageSpy.STATISTIC_GROUP.TO_SWITCH_SUBMIT_SUCCESS);

                final Port inputPort = input.getUpdatedPort().getPort().getPort().get(0);
                final PortModInput ofPortModInput = PortConvertor.toPortModInput(inputPort, getVersion());
                final PortModInputBuilder mdInput = new PortModInputBuilder(ofPortModInput);
                final Xid xid = requestContext.getXid();
                final OutboundQueue outboundQueue = getDeviceContext().getPrimaryConnectionContext().getOutboundQueueProvider();

                mdInput.setXid(xid.getValue());
                final SettableFuture<RpcResult<Void>> settableFuture = SettableFuture.create();
                outboundQueue.commitEntry(xid.getValue(), mdInput.build(), new FutureCallback<OfHeader>() {
                    @Override
                    public void onSuccess(final OfHeader ofHeader) {
                        RequestContextUtil.closeRequstContext(requestContext);
                        getDeviceContext().unhookRequestCtx(requestContext.getXid());
                        getMessageSpy().spyMessage(FlowModInput.class, MessageSpy.STATISTIC_GROUP.TO_SWITCH_SUBMIT_SUCCESS);

                        settableFuture.set(RpcResultBuilder.<Void>success().build());
                    }

                    @Override
                    public void onFailure(final Throwable throwable) {
                        RpcResultBuilder rpcResultBuilder = RpcResultBuilder.<Void>failed().withError(RpcError.ErrorType.APPLICATION, throwable.getMessage(), throwable);
                        RequestContextUtil.closeRequstContext(requestContext);
                        getDeviceContext().unhookRequestCtx(requestContext.getXid());
                        settableFuture.set(rpcResultBuilder.build());
                    }
                });
                return settableFuture;
            }
        });
    }

}
