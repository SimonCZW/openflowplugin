/**
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.openflowplugin.impl.connection.listener;

import java.util.concurrent.Future;
import org.opendaylight.openflowjava.protocol.api.connection.ConnectionReadyListener;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionContext;
import org.opendaylight.openflowplugin.api.openflow.connection.HandshakeContext;
import org.opendaylight.openflowplugin.impl.connection.HandshakeStepWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oneshot listener - once connection is ready, initiate handshake (if not already started by device).
 */
public class ConnectionReadyListenerImpl implements ConnectionReadyListener {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionReadyListenerImpl.class);

    private ConnectionContext connectionContext;
    private HandshakeContext handshakeContext;

    /**
     * Constructor.
     *
     * @param connectionContext - connection context
     * @param handshakeContext - handshake context
     */
    public ConnectionReadyListenerImpl(ConnectionContext connectionContext, HandshakeContext handshakeContext) {
        this.connectionContext = connectionContext;
        this.handshakeContext = handshakeContext;
    }

    /*
        当connection准备好,调用:
            1.会修改 connectionContext的状态为HANDSHAKING
            2.创建 HandshakeStepWrapper(), 最终调用HandshakeManagerImpl.shake()

        在TcpChannelInitializer的initchannel方法会调用ConnectionAdapterImpl.fireConnectionReadyNotification(),此方法会调用当前方法onConnectionReady()
     */
    @Override
    @SuppressWarnings("checkstyle:IllegalCatch")
    public void onConnectionReady() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("device is connected and ready-to-use (pipeline prepared): {}",
                    connectionContext.getConnectionAdapter().getRemoteAddress());
        }

        if (connectionContext.getConnectionState() == null) {
            synchronized (connectionContext) {
                if (connectionContext.getConnectionState() == null) {
                    connectionContext.changeStateToHandshaking();
                    /*
                        创建HandshakeStepWrapper(), 实际上是会调用HandshakeManagerImpl的shake()方法
                        handshakeContext.getHandshakeManager(): HandshakeManagerImpl
                        connectionContext.getConnectionAdapter()是底层TcpChannelInitializer传入的(封装当前连上的sw的channel)
                     */
                    HandshakeStepWrapper handshakeStepWrapper = new HandshakeStepWrapper(
                            null, handshakeContext.getHandshakeManager(), connectionContext.getConnectionAdapter());
                    // 调用线程运行
                    final Future<?> handshakeResult = handshakeContext.getHandshakePool().submit(handshakeStepWrapper);

                    try {
                        // As we run not in netty thread,
                        // need to remain in sync lock until initial handshake step processed.
                        handshakeResult.get();
                    } catch (Exception e) {
                        LOG.error("failed to process onConnectionReady event on device {}, reason {}",
                                connectionContext.getConnectionAdapter().getRemoteAddress(),
                                e);
                        connectionContext.closeConnection(false);
                        handshakeContext.close();
                    }
                } else {
                    LOG.debug("already touched by hello message from device {} after second check",
                            connectionContext.getConnectionAdapter().getRemoteAddress());
                }
            }
        } else {
            LOG.debug("already touched by hello message from device {} after first check",
                    connectionContext.getConnectionAdapter().getRemoteAddress());
        }
    }

}
