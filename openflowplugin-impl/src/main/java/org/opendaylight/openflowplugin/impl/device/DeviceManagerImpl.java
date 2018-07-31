/**
 * Copyright (c) 2015, 2017 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.openflowplugin.impl.device;

import com.google.common.util.concurrent.ListenableFuture;
import io.netty.util.HashedWheelTimer;
import io.netty.util.internal.ConcurrentSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.NotificationPublishService;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.openflowjava.protocol.api.connection.OutboundQueueHandlerRegistration;
import org.opendaylight.openflowplugin.api.openflow.OFPContext;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionContext;
import org.opendaylight.openflowplugin.api.openflow.connection.OutboundQueueProvider;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceContext;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceInfo;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceManager;
import org.opendaylight.openflowplugin.api.openflow.device.TranslatorLibrary;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainHolder;
import org.opendaylight.openflowplugin.api.openflow.statistics.ofpspecific.MessageSpy;
import org.opendaylight.openflowplugin.extension.api.ExtensionConverterProviderKeeper;
import org.opendaylight.openflowplugin.extension.api.core.extension.ExtensionConverterProvider;
import org.opendaylight.openflowplugin.impl.connection.OutboundQueueProviderImpl;
import org.opendaylight.openflowplugin.impl.device.initialization.DeviceInitializerProvider;
import org.opendaylight.openflowplugin.impl.device.listener.OpenflowProtocolListenerFullImpl;
import org.opendaylight.openflowplugin.impl.util.DeviceInitializationUtil;
import org.opendaylight.openflowplugin.openflow.md.core.sal.convertor.ConvertorExecutor;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemovedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeUpdatedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.openflow.provider.config.rev160510.OpenflowProviderConfig;
import org.opendaylight.yangtools.yang.binding.KeyedInstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceManagerImpl implements DeviceManager, ExtensionConverterProviderKeeper {

    private static final Logger LOG = LoggerFactory.getLogger(DeviceManagerImpl.class);
    private static final int SPY_RATE = 10;

    private final OpenflowProviderConfig config;
    private final DataBroker dataBroker;
    private final DeviceInitializerProvider deviceInitializerProvider;
    private final ConvertorExecutor convertorExecutor;
    private final ConcurrentMap<DeviceInfo, DeviceContext> deviceContexts = new ConcurrentHashMap<>();
    private final Set<KeyedInstanceIdentifier<Node, NodeKey>> notificationCreateNodeSend = new ConcurrentSet<>();
    private final NotificationPublishService notificationPublishService;
    private final MessageSpy messageSpy;
    private final HashedWheelTimer hashedWheelTimer;
    private final Object updatePacketInRateLimitersLock = new Object();
    private TranslatorLibrary translatorLibrary;
    private ExtensionConverterProvider extensionConverterProvider;
    private ScheduledThreadPoolExecutor spyPool;
    private ContextChainHolder contextChainHolder;

    public DeviceManagerImpl(@Nonnull final OpenflowProviderConfig config,
                             @Nonnull final DataBroker dataBroker,
                             @Nonnull final MessageSpy messageSpy,
                             @Nonnull final NotificationPublishService notificationPublishService,
                             @Nonnull final HashedWheelTimer hashedWheelTimer,
                             @Nonnull final ConvertorExecutor convertorExecutor,
                             @Nonnull final DeviceInitializerProvider deviceInitializerProvider) {
        this.config = config;
        this.dataBroker = dataBroker;
        this.deviceInitializerProvider = deviceInitializerProvider;
        this.convertorExecutor = convertorExecutor;
        this.hashedWheelTimer = hashedWheelTimer;
        this.spyPool = new ScheduledThreadPoolExecutor(1);
        this.notificationPublishService = notificationPublishService;
        this.messageSpy = messageSpy;
        DeviceInitializationUtil.makeEmptyNodes(dataBroker);
    }

    @Override
    public TranslatorLibrary oook() {
        return translatorLibrary;
    }

    @Override
    public void setTranslatorLibrary(final TranslatorLibrary translatorLibrary) {
        this.translatorLibrary = translatorLibrary;
    }

    @Override
    public void close() {
        deviceContexts.values().forEach(OFPContext::close);
        deviceContexts.clear();
        Optional.ofNullable(spyPool).ifPresent(ScheduledThreadPoolExecutor::shutdownNow);
        spyPool = null;

    }

    // 在OpenflowPluginProviderImpl的initialize中调用，调度线程池给消息监听器
    // messageSpy是在OpenflowPluginProviderImpl创建时传入的是：MessageIntelligenceAgencyImpl-消息监听Impl
    @Override
    public void initialize() {
        spyPool.scheduleAtFixedRate(messageSpy, SPY_RATE, SPY_RATE, TimeUnit.SECONDS);
    }

    @Override
    public void setExtensionConverterProvider(final ExtensionConverterProvider extensionConverterProvider) {
        this.extensionConverterProvider = extensionConverterProvider;
    }

    @Override
    public ExtensionConverterProvider getExtensionConverterProvider() {
        return extensionConverterProvider;
    }

    @Override
    public ListenableFuture<Void> removeDeviceFromOperationalDS(
            @Nonnull final KeyedInstanceIdentifier<Node, NodeKey> ii) {

        final WriteTransaction delWtx = dataBroker.newWriteOnlyTransaction();
        delWtx.delete(LogicalDatastoreType.OPERATIONAL, ii);
        return delWtx.submit();

    }

    /*
        ContextChainHolderImpl中调用, 在sw连上控制器handshake后才会调用
     */
    @Override
    public DeviceContext createContext(@Nonnull final ConnectionContext connectionContext) {

        LOG.info("ConnectionEvent: Device connected to controller, Device:{}, NodeId:{}",
                connectionContext.getConnectionAdapter().getRemoteAddress(),
                connectionContext.getDeviceInfo().getNodeId());

        // 过滤掉packetIn消息
        connectionContext.getConnectionAdapter().setPacketInFiltering(true);

        final OutboundQueueProvider outboundQueueProvider
                = new OutboundQueueProviderImpl(connectionContext.getDeviceInfo().getVersion());

        connectionContext.setOutboundQueueProvider(outboundQueueProvider);
        // 注册OutboundQueueHandler，用于output先到OutboundQueueHandler再会到ConnectionAdapterImpl
        final OutboundQueueHandlerRegistration<OutboundQueueProvider> outboundQueueHandlerRegistration =
                connectionContext.getConnectionAdapter().registerOutboundQueueHandler(
                        outboundQueueProvider,
                        config.getBarrierCountLimit().getValue(), // 25600
                        TimeUnit.MILLISECONDS.toNanos(config.getBarrierIntervalTimeoutLimit().getValue())); // 500
        connectionContext.setOutboundQueueHandleRegistration(outboundQueueHandlerRegistration);


        final DeviceContext deviceContext = new DeviceContextImpl(
                connectionContext,
                dataBroker,
                messageSpy,
                translatorLibrary,
                convertorExecutor,
                config.isSkipTableFeatures(),
                hashedWheelTimer,
                config.isUseSingleLayerSerialization(),
                deviceInitializerProvider,
                config.isEnableFlowRemovedNotification(),
                config.isSwitchFeaturesMandatory(),
                contextChainHolder);

        ((ExtensionConverterProviderKeeper) deviceContext).setExtensionConverterProvider(extensionConverterProvider);
        deviceContext.setNotificationPublishService(notificationPublishService);

        // 索引deviceContext
        deviceContexts.put(connectionContext.getDeviceInfo(), deviceContext);
        updatePacketInRateLimiters();

        // 创建message监听器
        final OpenflowProtocolListenerFullImpl messageListener = new OpenflowProtocolListenerFullImpl(
                connectionContext.getConnectionAdapter(), deviceContext);

        /*
            注册messageListener到ConnectionAdapterImpl中,
                当ConnectionAdapterImpl有消息会回调OpenflowProtocolListenerFullImpl messageListener的方法
         */
        connectionContext.getConnectionAdapter().setMessageListener(messageListener);
        connectionContext.getConnectionAdapter().setAlienMessageListener(messageListener);

        return deviceContext;
    }

    private void updatePacketInRateLimiters() {
        synchronized (updatePacketInRateLimitersLock) {
            final int deviceContextsSize = deviceContexts.size();
            if (deviceContextsSize > 0) {
                long freshNotificationLimit = config.getGlobalNotificationQuota() / deviceContextsSize;
                if (freshNotificationLimit < 100) {
                    freshNotificationLimit = 100;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("fresh notification limit = {}", freshNotificationLimit);
                }
                for (final DeviceContext deviceContext : deviceContexts.values()) {
                    deviceContext.updatePacketInRateLimit(freshNotificationLimit);
                }
            }
        }
    }

    @Override
    public void onDeviceRemoved(final DeviceInfo deviceInfo) {
        deviceContexts.remove(deviceInfo);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Device context removed for node {}", deviceInfo);
        }

        this.updatePacketInRateLimiters();
    }

    @Override
    public void sendNodeRemovedNotification(@Nonnull final KeyedInstanceIdentifier<Node, NodeKey> instanceIdentifier) {
        if (notificationCreateNodeSend.remove(instanceIdentifier)) {
            NodeRemovedBuilder builder = new NodeRemovedBuilder();
            builder.setNodeRef(new NodeRef(instanceIdentifier));
            LOG.info("Publishing node removed notification for {}", instanceIdentifier.firstKeyOf(Node.class).getId());
            /*
                在opendaylight-inventory.yang中看到此方式不会删除节点，仅通知，已被废弃Deprecated

                实际删除operational yang node位置在: ContextChainHolderImpl.ownershipChanged 方法
             */
            notificationPublishService.offerNotification(builder.build());
        }
    }

    @Override
    public void setContextChainHolder(@Nonnull ContextChainHolder contextChainHolder) {
        this.contextChainHolder = contextChainHolder;
    }

    /*
        在contextChainHolderImpl中 节点作为一个contextChain的leader运行起来后, 几个context都初始化完成, 已经确定master为当前节点后调用
     */
    @Override
    public void sendNodeAddedNotification(@Nonnull final KeyedInstanceIdentifier<Node, NodeKey> instanceIdentifier) {
        if (!notificationCreateNodeSend.contains(instanceIdentifier)) {
            notificationCreateNodeSend.add(instanceIdentifier);
            final NodeId id = instanceIdentifier.firstKeyOf(Node.class).getId();
            /*
             inventory.rev130819.NodeUpdatedBuilder

                     description "A notification sent by someone who realized there was a modification to a node, but did not modify the data tree.
                    Describes that something on the node has been updated (including addition of a new node), but is for
                    whatever reason is not modifying the data tree.
                    Deprecated: If a process determines that a node was updated, then that
                    logic should update the node using the DataBroker directly. Listeners interested
                    update changes should register a data change listener for notifications on removals.";

             */
            NodeUpdatedBuilder builder = new NodeUpdatedBuilder();
            builder.setId(id);
            builder.setNodeRef(new NodeRef(instanceIdentifier));
            /*
                在opendaylight-inventory.yang中看到此方式不会新增节点，仅通知，已被废弃Deprecated

                实际新增/写入operational yang node位置: 在DeviceContext.instantiateServiceInstance中调用initializer.initialize()处: AbstractDeviceInitializer.initialize()方法写入inventory node
             */
            LOG.info("Publishing node added notification for {}", id);

            // node updated通知
            notificationPublishService.offerNotification(builder.build());
        }
    }
}
