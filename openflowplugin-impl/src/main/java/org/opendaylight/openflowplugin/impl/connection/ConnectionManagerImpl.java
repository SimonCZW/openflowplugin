/**
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.openflowplugin.impl.connection;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import org.opendaylight.openflowjava.protocol.api.connection.ConnectionAdapter;
import org.opendaylight.openflowjava.protocol.api.connection.ConnectionReadyListener;
import org.opendaylight.openflowplugin.api.OFConstants;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionContext;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionManager;
import org.opendaylight.openflowplugin.api.openflow.connection.HandshakeContext;
import org.opendaylight.openflowplugin.api.openflow.device.handlers.DeviceConnectedHandler;
import org.opendaylight.openflowplugin.api.openflow.device.handlers.DeviceDisconnectedHandler;
import org.opendaylight.openflowplugin.api.openflow.md.core.HandshakeListener;
import org.opendaylight.openflowplugin.api.openflow.md.core.HandshakeManager;
import org.opendaylight.openflowplugin.impl.common.DeviceConnectionRateLimiter;
import org.opendaylight.openflowplugin.impl.connection.listener.ConnectionReadyListenerImpl;
import org.opendaylight.openflowplugin.impl.connection.listener.HandshakeListenerImpl;
import org.opendaylight.openflowplugin.impl.connection.listener.OpenflowProtocolListenerInitialImpl;
import org.opendaylight.openflowplugin.impl.connection.listener.SystemNotificationsListenerImpl;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.OpenflowProtocolListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.system.rev130927.SystemNotificationsListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.openflow.provider.config.rev160510.OpenflowProviderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManagerImpl implements ConnectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManagerImpl.class);
    private static final boolean BITMAP_NEGOTIATION_ENABLED = true;
    private DeviceConnectedHandler deviceConnectedHandler;
    private final OpenflowProviderConfig config;
    private final ExecutorService executorService;
    private final DeviceConnectionRateLimiter deviceConnectionRateLimiter;
    private DeviceDisconnectedHandler deviceDisconnectedHandler;

    public ConnectionManagerImpl(final OpenflowProviderConfig config, final ExecutorService executorService) {
        this.config = config;
        this.executorService = executorService;
        this.deviceConnectionRateLimiter = new DeviceConnectionRateLimiter(config);
    }

    /*
        实际上处理是，给从底层连接建立的TcpChannelInitializer时调用此方法传递的connectionAdapter对象传入对象:
            1.设置ConnectionReadyListenerImpl, 提供onConnectionReady(). onConnectionReady()处理是:HandshakeManagerImpl.shake()
            2.设置ofMessageListener(监听openflow消息处理)
            3.设置SystemNotificationsListenerImpl, 提供onSwitchIdleEvent()方法, 当swich idle发送echo心跳消息
     */
    @Override
    public void onSwitchConnected(final ConnectionAdapter connectionAdapter) {
        LOG.trace("prepare connection context");
        // connectionAdapter是：ConnectionAdapterImpl: 包含基本的socket channel信息

        // 创建ConnectionContext
        final ConnectionContext connectionContext = new ConnectionContextImpl(connectionAdapter);
        connectionContext.setDeviceDisconnectedHandler(this.deviceDisconnectedHandler);

        // 创建HandshakeListenerImpl, 当handshake成功,会调用 deviceConnectedHandler.deviceConnected()
        //      deviceConnectedHandlerContext是在OpenflowPluginProvider中设置的ContextChainHolderImpl
        HandshakeListener handshakeListener = new HandshakeListenerImpl(connectionContext, deviceConnectedHandler);

        // 创建HandshakeManagerImpl: 提供shake()等方法实现握手
        final HandshakeManager handshakeManager = createHandshakeManager(connectionAdapter, handshakeListener);

        LOG.trace("prepare handshake context");
        // manager传入context
        HandshakeContext handshakeContext = new HandshakeContextImpl(executorService, handshakeManager);
        // 设置handshakeContext,用于当handshake失败时调用close()方法
        handshakeListener.setHandshakeContext(handshakeContext);
        connectionContext.setHandshakeContext(handshakeContext);

        // 至此connectionContext封装了 connectionAdapter, handshakeContext

        LOG.trace("prepare connection listeners");
        /*
            传入handshakeContext, 提供onConnectionReady()方法. onConnectionReady()效果:
                1.会修改 connectionContext的状态为HANDSHAKING
                2.创建 HandshakeStepWrapper(), 最终调用HandshakeManagerImpl.shake()
          */
        final ConnectionReadyListener connectionReadyListener = new ConnectionReadyListenerImpl(
                connectionContext, handshakeContext);
        // 给底层TcpCannelInitializer传递上来的connectionAdapter 设置connectionReadyListener
        connectionAdapter.setConnectionReadyListener(connectionReadyListener);

        // 给底层TcpCannelInitializer传递上来的connectionAdapter 设置ofMessageListener(监听openflow消息处理)
        final OpenflowProtocolListener ofMessageListener =
                new OpenflowProtocolListenerInitialImpl(connectionContext, handshakeContext);
        connectionAdapter.setMessageListener(ofMessageListener);

        // 提供onSwitchIdleEvent()方法, 当swich idle发送echo心跳消息
        final SystemNotificationsListener systemListener = new SystemNotificationsListenerImpl(
                connectionContext, config.getEchoReplyTimeout().getValue(), executorService);
        // 给底层TcpCannelInitializer传递上来的connectionAdapter 设置systemListener
        connectionAdapter.setSystemListener(systemListener);

        LOG.trace("connection ballet finished");
    }

    // HandshakeManagerImpl实现HandshakeManage
    private HandshakeManager createHandshakeManager(final ConnectionAdapter connectionAdapter,
                                                    final HandshakeListener handshakeListener) {
        HandshakeManagerImpl handshakeManager = new HandshakeManagerImpl(connectionAdapter,
                OFConstants.VERSION_ORDER.get(0),
                OFConstants.VERSION_ORDER, new ErrorHandlerSimpleImpl(), handshakeListener, BITMAP_NEGOTIATION_ENABLED,
                deviceConnectionRateLimiter);

        return handshakeManager;
    }

    @Override
    public boolean accept(final InetAddress switchAddress) {
        // TODO add connection accept logic based on address
        return true;
    }

    @Override
    public void setDeviceConnectedHandler(final DeviceConnectedHandler deviceConnectedHandler) {
        this.deviceConnectedHandler = deviceConnectedHandler;
    }

    @Override
    public void setDeviceDisconnectedHandler(final DeviceDisconnectedHandler deviceDisconnectedHandler) {
        this.deviceDisconnectedHandler = deviceDisconnectedHandler;
    }
}
