/**
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.openflowplugin.impl.device;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.HashedWheelTimer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.NotificationPublishService;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.mdsal.singleton.common.api.ServiceGroupIdentifier;
import org.opendaylight.openflowjava.protocol.api.connection.ConnectionAdapter;
import org.opendaylight.openflowjava.protocol.api.keys.MessageTypeKey;
import org.opendaylight.openflowplugin.api.OFConstants;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionContext;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceContext;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceInfo;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceState;
import org.opendaylight.openflowplugin.api.openflow.device.MessageTranslator;
import org.opendaylight.openflowplugin.api.openflow.device.RequestContext;
import org.opendaylight.openflowplugin.api.openflow.device.TranslatorLibrary;
import org.opendaylight.openflowplugin.api.openflow.device.Xid;
import org.opendaylight.openflowplugin.api.openflow.device.handlers.MultiMsgCollector;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChain;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainHolder;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainMastershipState;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainMastershipWatcher;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainState;
import org.opendaylight.openflowplugin.api.openflow.md.core.TranslatorKey;
import org.opendaylight.openflowplugin.api.openflow.md.util.OpenflowVersion;
import org.opendaylight.openflowplugin.api.openflow.registry.flow.DeviceFlowRegistry;
import org.opendaylight.openflowplugin.api.openflow.registry.group.DeviceGroupRegistry;
import org.opendaylight.openflowplugin.api.openflow.registry.meter.DeviceMeterRegistry;
import org.opendaylight.openflowplugin.api.openflow.statistics.ofpspecific.MessageSpy;
import org.opendaylight.openflowplugin.common.txchain.TransactionChainManager;
import org.opendaylight.openflowplugin.extension.api.ConvertorMessageFromOFJava;
import org.opendaylight.openflowplugin.extension.api.ExtensionConverterProviderKeeper;
import org.opendaylight.openflowplugin.extension.api.core.extension.ExtensionConverterProvider;
import org.opendaylight.openflowplugin.extension.api.exception.ConversionException;
import org.opendaylight.openflowplugin.extension.api.path.MessagePath;
import org.opendaylight.openflowplugin.impl.datastore.MultipartWriterProvider;
import org.opendaylight.openflowplugin.impl.datastore.MultipartWriterProviderFactory;
import org.opendaylight.openflowplugin.impl.device.initialization.AbstractDeviceInitializer;
import org.opendaylight.openflowplugin.impl.device.initialization.DeviceInitializerProvider;
import org.opendaylight.openflowplugin.impl.device.listener.MultiMsgCollectorImpl;
import org.opendaylight.openflowplugin.impl.registry.flow.DeviceFlowRegistryImpl;
import org.opendaylight.openflowplugin.impl.registry.group.DeviceGroupRegistryImpl;
import org.opendaylight.openflowplugin.impl.registry.meter.DeviceMeterRegistryImpl;
import org.opendaylight.openflowplugin.impl.rpc.AbstractRequestContext;
import org.opendaylight.openflowplugin.impl.services.util.RequestContextUtil;
import org.opendaylight.openflowplugin.impl.util.MatchUtil;
import org.opendaylight.openflowplugin.openflow.md.core.sal.convertor.ConvertorExecutor;
import org.opendaylight.openflowplugin.openflow.md.util.InventoryDataServiceUtil;
import org.opendaylight.yang.gen.v1.urn.opendaylight.experimenter.message.service.rev151020.ExperimenterMessageFromDevBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.Match;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.common.types.rev130731.PortReason;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.Error;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.ExperimenterMessage;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.FlowRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.OfHeader;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.PacketInMessage;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.PortGrouping;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.PortStatus;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.PortStatusMessage;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.experimenter.core.ExperimenterDataOfChoice;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflowplugin.experimenter.types.rev151020.experimenter.core.message.ExperimenterMessageOfChoice;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketIn;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceivedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.port.statistics.rev131214.FlowCapableNodeConnectorStatisticsData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.port.statistics.rev131214.FlowCapableNodeConnectorStatisticsDataBuilder;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.KeyedInstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceContextImpl implements DeviceContext, ExtensionConverterProviderKeeper {

    private static final Logger LOG = LoggerFactory.getLogger(DeviceContextImpl.class);

    // TODO: drain factor should be parametrized
    private static final float REJECTED_DRAIN_FACTOR = 0.25f;
    // TODO: low water mark factor should be parametrized
    private static final float LOW_WATERMARK_FACTOR = 0.75f;
    // TODO: high water mark factor should be parametrized
    private static final float HIGH_WATERMARK_FACTOR = 0.95f;

    // Timeout in milliseconds after what we will give up on initializing device
    private static final int DEVICE_INIT_TIMEOUT = 9000;

    // Timeout in milliseconds after what we will give up on closing transaction chain
    private static final int TX_CHAIN_CLOSE_TIMEOUT = 10000;

    private static final int LOW_WATERMARK = 1000;
    private static final int HIGH_WATERMARK = 2000;

    private final MultipartWriterProvider writerProvider;
    private final HashedWheelTimer hashedWheelTimer;
    private final DeviceState deviceState;
    private final DataBroker dataBroker;
    private final Collection<RequestContext<?>> requestContexts = new HashSet<>();
    private final MessageSpy messageSpy;
    private final MessageTranslator<PortGrouping, FlowCapableNodeConnector> portStatusTranslator;
    private final MessageTranslator<PacketInMessage, PacketReceived> packetInTranslator;
    private final MessageTranslator<FlowRemoved, org.opendaylight.yang.gen.v1.urn.opendaylight
            .flow.service.rev130819.FlowRemoved> flowRemovedTranslator;
    private final TranslatorLibrary translatorLibrary;
    private final ConvertorExecutor convertorExecutor;
    private final DeviceInitializerProvider deviceInitializerProvider;
    private final PacketInRateLimiter packetInLimiter;
    private final DeviceInfo deviceInfo;
    private final ConnectionContext primaryConnectionContext;
    private final boolean skipTableFeatures;
    private final boolean switchFeaturesMandatory;
    private final boolean isFlowRemovedNotificationOn;
    private final boolean useSingleLayerSerialization;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean hasState = new AtomicBoolean(false);
    private final AtomicBoolean isInitialTransactionSubmitted = new AtomicBoolean(false);
    private final ContextChainHolder contextChainHolder;
    private NotificationPublishService notificationPublishService;
    private TransactionChainManager transactionChainManager;
    private DeviceFlowRegistry deviceFlowRegistry;
    private DeviceGroupRegistry deviceGroupRegistry;
    private DeviceMeterRegistry deviceMeterRegistry;
    private ExtensionConverterProvider extensionConverterProvider;
    private ContextChainMastershipWatcher contextChainMastershipWatcher;

    DeviceContextImpl(@Nonnull final ConnectionContext primaryConnectionContext,
                      @Nonnull final DataBroker dataBroker,
                      @Nonnull final MessageSpy messageSpy,
                      @Nonnull final TranslatorLibrary translatorLibrary,
                      final ConvertorExecutor convertorExecutor,
                      final boolean skipTableFeatures,
                      final HashedWheelTimer hashedWheelTimer,
                      final boolean useSingleLayerSerialization,
                      final DeviceInitializerProvider deviceInitializerProvider,
                      final boolean isFlowRemovedNotificationOn,
                      final boolean switchFeaturesMandatory,
                      final ContextChainHolder contextChainHolder) {

        this.primaryConnectionContext = primaryConnectionContext;
        this.deviceInfo = primaryConnectionContext.getDeviceInfo();
        this.hashedWheelTimer = hashedWheelTimer;
        this.deviceInitializerProvider = deviceInitializerProvider; // DeviceInitializerProviderFactory.createDefaultProvider();
        this.isFlowRemovedNotificationOn = isFlowRemovedNotificationOn;
        this.switchFeaturesMandatory = switchFeaturesMandatory;
        this.deviceState = new DeviceStateImpl();
        this.dataBroker = dataBroker;
        this.messageSpy = messageSpy;
        this.contextChainHolder = contextChainHolder;

        this.packetInLimiter = new PacketInRateLimiter(primaryConnectionContext.getConnectionAdapter(),
                /*initial*/ LOW_WATERMARK, /*initial*/HIGH_WATERMARK, this.messageSpy, REJECTED_DRAIN_FACTOR);

        this.translatorLibrary = translatorLibrary;
        this.portStatusTranslator = translatorLibrary.lookupTranslator(
                new TranslatorKey(deviceInfo.getVersion(), PortGrouping.class.getName()));
        this.packetInTranslator = translatorLibrary.lookupTranslator(
                new TranslatorKey(deviceInfo.getVersion(), org.opendaylight.yang.gen.v1.urn.opendaylight.openflow
                        .protocol.rev130731
                        .PacketIn.class.getName()));
        this.flowRemovedTranslator = translatorLibrary.lookupTranslator(
                new TranslatorKey(deviceInfo.getVersion(), FlowRemoved.class.getName()));

        this.convertorExecutor = convertorExecutor;
        this.skipTableFeatures = skipTableFeatures;
        this.useSingleLayerSerialization = useSingleLayerSerialization;
        writerProvider = MultipartWriterProviderFactory.createDefaultProvider(this);
    }

    @Override
    public boolean initialSubmitTransaction() {
        if (!initialized.get()) {
            return false;
        }

        final boolean initialSubmit = transactionChainManager.initialSubmitWriteTransaction();
        isInitialTransactionSubmitted.set(initialSubmit);
        return initialSubmit;
    }

    @Override
    public DeviceState getDeviceState() {
        return deviceState;
    }

    @Override
    public ReadOnlyTransaction getReadTransaction() {
        return dataBroker.newReadOnlyTransaction();
    }

    @Override
    public boolean isTransactionsEnabled() {
        return isInitialTransactionSubmitted.get();
    }

    @Override
    public <T extends DataObject> void writeToTransaction(final LogicalDatastoreType store,
                                                          final InstanceIdentifier<T> path,
                                                          final T data) {
        if (initialized.get()) {
            transactionChainManager.writeToTransaction(store, path, data, false);
        }
    }

    @Override
    public <T extends DataObject> void writeToTransactionWithParentsSlow(final LogicalDatastoreType store,
                                                                         final InstanceIdentifier<T> path,
                                                                         final T data) {
        if (initialized.get()) {
            transactionChainManager.writeToTransaction(store, path, data, true);
        }
    }

    @Override
    public <T extends DataObject> void addDeleteToTxChain(final LogicalDatastoreType store,
                                                          final InstanceIdentifier<T> path) {
        if (initialized.get()) {
            transactionChainManager.addDeleteOperationToTxChain(store, path);
        }
    }

    @Override
    public boolean submitTransaction() {
        return initialized.get() && transactionChainManager.submitTransaction();
    }

    @Override
    public boolean syncSubmitTransaction() {
        return initialized.get() && transactionChainManager.submitTransaction(true);
    }

    @Override
    public ConnectionContext getPrimaryConnectionContext() {
        return primaryConnectionContext;
    }

    @Override
    public DeviceFlowRegistry getDeviceFlowRegistry() {
        return deviceFlowRegistry;
    }

    @Override
    public DeviceGroupRegistry getDeviceGroupRegistry() {
        return deviceGroupRegistry;
    }

    @Override
    public DeviceMeterRegistry getDeviceMeterRegistry() {
        return deviceMeterRegistry;
    }

    @Override
    public void processReply(final OfHeader ofHeader) {
        messageSpy.spyMessage(
                ofHeader.getImplementedInterface(),
                ofHeader instanceof Error
                        ? MessageSpy.StatisticsGroup.FROM_SWITCH_PUBLISHED_FAILURE
                        : MessageSpy.StatisticsGroup.FROM_SWITCH_PUBLISHED_SUCCESS);
    }

    @Override
    public void processReply(final Xid xid, final List<? extends OfHeader> ofHeaderList) {
        ofHeaderList.forEach(header -> messageSpy.spyMessage(
                header.getImplementedInterface(),
                header instanceof Error
                        ? MessageSpy.StatisticsGroup.FROM_SWITCH_PUBLISHED_FAILURE
                        : MessageSpy.StatisticsGroup.FROM_SWITCH_PUBLISHED_SUCCESS));
    }

    @Override
    public void processFlowRemovedMessage(final FlowRemoved flowRemoved) {
        if (isMasterOfDevice()) {
            //1. translate to general flow (table, priority, match, cookie)
            final org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819
                    .FlowRemoved flowRemovedNotification = flowRemovedTranslator
                    .translate(flowRemoved, deviceInfo, null);

            if (isFlowRemovedNotificationOn) {
                // Trigger off a notification
                notificationPublishService.offerNotification(flowRemovedNotification);
            }
        } else {
            LOG.debug("Controller is not owner of the device {}, skipping Flow Removed message",
                    deviceInfo.getLOGValue());
        }
    }

    @Override
    @SuppressWarnings("checkstyle:IllegalCatch")
    public void processPortStatusMessage(final PortStatusMessage portStatus) {
        messageSpy.spyMessage(portStatus.getImplementedInterface(), MessageSpy.StatisticsGroup
                .FROM_SWITCH_PUBLISHED_SUCCESS);

        if (initialized.get()) {
            try {
                writePortStatusMessage(portStatus);
            } catch (final Exception e) {
                LOG.warn("Error processing port status message for port {} on device {}",
                        portStatus.getPortNo(), getDeviceInfo(), e);
            }
        } else if (!hasState.get()) {
            primaryConnectionContext.handlePortStatusMessage(portStatus);
        }
    }

    private void writePortStatusMessage(final PortStatus portStatusMessage) {
        final FlowCapableNodeConnector flowCapableNodeConnector = portStatusTranslator
                .translate(portStatusMessage, getDeviceInfo(), null);

        final KeyedInstanceIdentifier<NodeConnector, NodeConnectorKey> iiToNodeConnector = getDeviceInfo()
                .getNodeInstanceIdentifier()
                .child(NodeConnector.class, new NodeConnectorKey(InventoryDataServiceUtil
                        .nodeConnectorIdfromDatapathPortNo(
                                deviceInfo.getDatapathId(),
                                portStatusMessage.getPortNo(),
                                OpenflowVersion.get(deviceInfo.getVersion()))));

        // 将port(nodeConnector)写入到它的ovs node的opernational YANG
        writeToTransaction(LogicalDatastoreType.OPERATIONAL, iiToNodeConnector, new NodeConnectorBuilder()
                .setKey(iiToNodeConnector.getKey())
                .addAugmentation(FlowCapableNodeConnectorStatisticsData.class, new
                        FlowCapableNodeConnectorStatisticsDataBuilder().build())
                .addAugmentation(FlowCapableNodeConnector.class, flowCapableNodeConnector)
                .build());
        syncSubmitTransaction();
        if (PortReason.OFPPRDELETE.equals(portStatusMessage.getReason())) {
            addDeleteToTxChain(LogicalDatastoreType.OPERATIONAL, iiToNodeConnector);
            syncSubmitTransaction();
        }
    }

    @Override
    public void processPacketInMessage(final PacketInMessage packetInMessage) {
        if (isMasterOfDevice()) {
            final PacketReceived packetReceived = packetInTranslator.translate(packetInMessage, getDeviceInfo(), null);
            handlePacketInMessage(packetReceived, packetInMessage.getImplementedInterface(), packetReceived.getMatch());
        } else {
            LOG.debug("Controller is not owner of the device {}, skipping packet_in message", deviceInfo.getLOGValue());
        }
    }

    private Boolean isMasterOfDevice() {
        final ContextChain contextChain = contextChainHolder.getContextChain(deviceInfo);
        boolean result = false;
        if (contextChain != null) {
            result = contextChain.isMastered(ContextChainMastershipState.CHECK, false);
        }
        return result;
    }

    private void handlePacketInMessage(final PacketIn packetIn,
                                       final Class<?> implementedInterface,
                                       final Match match) {
        messageSpy.spyMessage(implementedInterface, MessageSpy.StatisticsGroup.FROM_SWITCH);
        final ConnectionAdapter connectionAdapter = getPrimaryConnectionContext().getConnectionAdapter();

        if (packetIn == null) {
            LOG.debug("Received a null packet from switch {}", connectionAdapter.getRemoteAddress());
            messageSpy.spyMessage(implementedInterface, MessageSpy.StatisticsGroup.FROM_SWITCH_TRANSLATE_SRC_FAILURE);
            return;
        }

        final OpenflowVersion openflowVersion = OpenflowVersion.get(deviceInfo.getVersion());

        // Try to get ingress from match
        final NodeConnectorRef nodeConnectorRef = Objects.nonNull(packetIn.getIngress())
                ? packetIn.getIngress() : Optional.ofNullable(match)
                .map(Match::getInPort)
                .map(nodeConnectorId -> InventoryDataServiceUtil
                        .portNumberfromNodeConnectorId(
                                openflowVersion,
                                nodeConnectorId))
                .map(portNumber -> InventoryDataServiceUtil
                        .nodeConnectorRefFromDatapathIdPortno(
                                deviceInfo.getDatapathId(),
                                portNumber,
                                openflowVersion))
                .orElse(null);

        messageSpy.spyMessage(implementedInterface, MessageSpy.StatisticsGroup.FROM_SWITCH_TRANSLATE_OUT_SUCCESS);

        if (!packetInLimiter.acquirePermit()) {
            LOG.debug("Packet limited");
            // TODO: save packet into emergency slot if possible
            messageSpy.spyMessage(implementedInterface, MessageSpy.StatisticsGroup
                    .FROM_SWITCH_PACKET_IN_LIMIT_REACHED_AND_DROPPED);
            return;
        }

        final ListenableFuture<?> offerNotification = notificationPublishService
                .offerNotification(new PacketReceivedBuilder(packetIn)
                        .setIngress(nodeConnectorRef)
                        .setMatch(MatchUtil.transformMatch(match,
                                org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.packet.received
                                        .Match.class))
                        .build());

        if (NotificationPublishService.REJECTED.equals(offerNotification)) {
            LOG.debug("notification offer rejected");
            messageSpy.spyMessage(implementedInterface, MessageSpy.StatisticsGroup.FROM_SWITCH_NOTIFICATION_REJECTED);
            packetInLimiter.drainLowWaterMark();
            packetInLimiter.releasePermit();
            return;
        }

        Futures.addCallback(offerNotification, new FutureCallback<Object>() {
            @Override
            public void onSuccess(final Object result) {
                messageSpy.spyMessage(implementedInterface, MessageSpy.StatisticsGroup.FROM_SWITCH_PUBLISHED_SUCCESS);
                packetInLimiter.releasePermit();
            }

            @Override
            public void onFailure(final Throwable throwable) {
                messageSpy.spyMessage(implementedInterface, MessageSpy.StatisticsGroup
                        .FROM_SWITCH_NOTIFICATION_REJECTED);
                LOG.debug("notification offer failed: {}", throwable.getMessage());
                LOG.trace("notification offer failed..", throwable);
                packetInLimiter.releasePermit();
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public void processExperimenterMessage(final ExperimenterMessage notification) {
        if (isMasterOfDevice()) {
            // lookup converter
            final ExperimenterDataOfChoice vendorData = notification.getExperimenterDataOfChoice();
            final MessageTypeKey<? extends ExperimenterDataOfChoice> key = new MessageTypeKey<>(
                    getDeviceInfo().getVersion(),
                    (Class<? extends ExperimenterDataOfChoice>) vendorData.getImplementedInterface());
            final ConvertorMessageFromOFJava<ExperimenterDataOfChoice, MessagePath> messageConverter =
                    extensionConverterProvider.getMessageConverter(key);
            if (messageConverter == null) {
                LOG.warn("custom converter for {}[OF:{}] not found",
                        notification.getExperimenterDataOfChoice().getImplementedInterface(),
                        getDeviceInfo().getVersion());
                return;
            }
            // build notification
            final ExperimenterMessageOfChoice messageOfChoice;
            try {
                messageOfChoice = messageConverter.convert(vendorData, MessagePath.MESSAGE_NOTIFICATION);
                final ExperimenterMessageFromDevBuilder experimenterMessageFromDevBld = new
                        ExperimenterMessageFromDevBuilder()
                        .setNode(new NodeRef(getDeviceInfo().getNodeInstanceIdentifier()))
                        .setExperimenterMessageOfChoice(messageOfChoice);
                // publish
                notificationPublishService.offerNotification(experimenterMessageFromDevBld.build());
            } catch (final ConversionException e) {
                LOG.error("Conversion of experimenter notification failed", e);
            }
        } else {
            LOG.debug("Controller is not owner of the device {}, skipping experimenter message",
                    deviceInfo.getLOGValue());
        }
    }

    @Override
    // The cast to PacketInMessage is safe as the implemented interface is verified before the cas tbut FB doesn't
    // recognize it.
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public boolean processAlienMessage(final OfHeader message) {
        final Class<? extends DataContainer> implementedInterface = message.getImplementedInterface();

        if (Objects.nonNull(implementedInterface) && org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service
                .rev130709.PacketInMessage.class.equals(implementedInterface)) {
            final org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709
                    .PacketInMessage packetInMessage = org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service
                    .rev130709.PacketInMessage.class.cast(message);

            handlePacketInMessage(packetInMessage, implementedInterface, packetInMessage.getMatch());
            return true;
        }

        return false;
    }

    @Override
    public TranslatorLibrary oook() {
        return translatorLibrary;
    }

    @Override
    public void setNotificationPublishService(final NotificationPublishService notificationPublishService) {
        this.notificationPublishService = notificationPublishService;
    }

    @Override
    public MessageSpy getMessageSpy() {
        return messageSpy;
    }

    @Override
    public void onPublished() {
        /*
            primaryConnectionContext是ConnectionContext
            从ConnectionContext中拿到南向的ConnectionAdapterImpl对象，设置packetIn filter为false

            在调用链的前面, 创建DeviceContext时设置了为true. 为的是创建contextChain过程中不接受packetIn?
         */
        primaryConnectionContext.getConnectionAdapter().setPacketInFiltering(false);
    }

    @Override
    public <T extends OfHeader> MultiMsgCollector<T> getMultiMsgCollector(final RequestContext<List<T>>
                                                                                      requestContext) {
        return new MultiMsgCollectorImpl<>(this, requestContext);
    }

    @Override
    public void updatePacketInRateLimit(final long upperBound) {
        packetInLimiter.changeWaterMarks((int) (LOW_WATERMARK_FACTOR * upperBound),
                (int) (HIGH_WATERMARK_FACTOR * upperBound));
    }

    @Override
    public void setExtensionConverterProvider(final ExtensionConverterProvider extensionConverterProvider) {
        this.extensionConverterProvider = extensionConverterProvider;
    }

    @Override
    public ExtensionConverterProvider getExtensionConverterProvider() {
        return extensionConverterProvider;
    }

    @VisibleForTesting
    TransactionChainManager getTransactionChainManager() {
        return this.transactionChainManager;
    }

    @Override
    public ListenableFuture<Void> closeServiceInstance() {
        final ListenableFuture<Void> listenableFuture = initialized.get()
                ? transactionChainManager.deactivateTransactionManager()
                : Futures.immediateFuture(null);

        hashedWheelTimer.newTimeout((timerTask) -> {
            if (!listenableFuture.isDone() && !listenableFuture.isCancelled()) {
                listenableFuture.cancel(true);
            }
        }, TX_CHAIN_CLOSE_TIMEOUT, TimeUnit.MILLISECONDS);

        return listenableFuture;
    }

    @Override
    public DeviceInfo getDeviceInfo() {
        return this.deviceInfo;
    }

    @Override
    public void registerMastershipWatcher(@Nonnull final ContextChainMastershipWatcher newWatcher) {
        this.contextChainMastershipWatcher = newWatcher; //是ContextChainHolderImpl
    }

    @Nonnull
    @Override
    public ServiceGroupIdentifier getIdentifier() {
        return deviceInfo.getServiceIdentifier();
    }

    @Override
    public void close() {
        // Close all datastore registries and transactions
        if (initialized.getAndSet(false)) {
            deviceGroupRegistry.close();
            deviceFlowRegistry.close();
            deviceMeterRegistry.close();

            final ListenableFuture<Void> txChainShuttingDown = transactionChainManager.shuttingDown();

            Futures.addCallback(txChainShuttingDown, new FutureCallback<Void>() {
                @Override
                public void onSuccess(@Nullable final Void result) {
                    transactionChainManager.close();
                    transactionChainManager = null;
                }

                @Override
                public void onFailure(final Throwable throwable) {
                    transactionChainManager.close();
                    transactionChainManager = null;
                }
            }, MoreExecutors.directExecutor());
        }

        requestContexts.forEach(requestContext -> RequestContextUtil
                .closeRequestContextWithRpcError(requestContext, "Connection closed."));
        requestContexts.clear();
    }

    @Override
    public boolean canUseSingleLayerSerialization() {
        return useSingleLayerSerialization && getDeviceInfo().getVersion() >= OFConstants.OFP_VERSION_1_3;
    }

    /*
        在contextChainHolderImpl中,创建的当前DeviceContextImpl对象,会被add到contextChainImpl中, 而contextChainImpl中会被注册为singletonService,
            当contextImpl在当前节点成为leader,会调用其自身的instantiateServiceInstance()方法,
            而它的instantiateServiceInstance()方法会调用DeviceContextImpl的instantiateServiceInstance方法
     */
    // TODO: exception handling should be fixed by using custom checked exception, never RuntimeExceptions
    @Override
    @SuppressWarnings({"checkstyle:IllegalCatch"})
    public void instantiateServiceInstance() {
        /*
            1.创建transactionChainManager
            2.创建deviceFlowRegistry
            3.创建deviceGroupRegistry
            4.创建deviceMeterRegistry
         */
        lazyTransactionManagerInitialization();

        try {
            // 从connectionContext取 portMessages
            final List<PortStatusMessage> portStatusMessages = primaryConnectionContext
                    .retrieveAndClearPortStatusMessages();

            /*
                将portStatusMessages写入operational YANG
                    写入node的nodeConnector
             */
            portStatusMessages.forEach(this::writePortStatusMessage);
            submitTransaction();
        } catch (final Exception ex) {
            throw new RuntimeException(String.format("Error processing port status messages from device %s: %s",
                    deviceInfo.toString(), ex.toString()), ex);
        }

        /*
            deviceInitializerProvider是 DeviceInitializerProvider (从openflowplugin传入)

            此处lookup会根据openflow版本找到对应initializer:
                1.0: OF10DeviceInitializer()
                1.3: OF13DeviceInitializer()
         */
        final Optional<AbstractDeviceInitializer> initializer = deviceInitializerProvider
                .lookup(deviceInfo.getVersion());

        if (initializer.isPresent()) {
            /*
                调用initializer.initialize()方法, 会调用到AbstractDeviceInitializer(上述1.0/1.3的initializer的父类)的initialize方法,效果
                    1.将ovs node写入operational YANG
                    2.多态,调用回子类的initializeNodeInformation()方法:
                        2.1 解析capabilities, 写会deviceContext.deviceState中
                        2.2 creates an instance of RequestContext for each type of feature: table, group, meter features and port descriptions
             */
            final Future<Void> initialize = initializer
                    .get()
                    .initialize(this, switchFeaturesMandatory, skipTableFeatures, writerProvider, convertorExecutor);

            try {
                initialize.get(DEVICE_INIT_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                initialize.cancel(true);
                throw new RuntimeException(String.format("Failed to initialize device %s in %ss: %s",
                        deviceInfo.toString(), String.valueOf(DEVICE_INIT_TIMEOUT / 1000), ex.toString()), ex);
            } catch (ExecutionException | InterruptedException ex) {
                throw new RuntimeException(
                        String.format("Device %s cannot be initialized: %s", deviceInfo.toString(), ex.toString()), ex);
            }
        } else {
            throw new RuntimeException(String.format("Unsupported version %s for device %s",
                    deviceInfo.getVersion(),
                    deviceInfo.toString()));
        }

        /*
            调用DeviceFlowRegistryImpl.fill()
            应该是从ovs node对应的YANG FlowCapableNode的信息(table,flow)读取出来,写入到deviceFlowRegistryFill对象
          */
        final ListenableFuture<List<com.google.common.base.Optional<FlowCapableNode>>> deviceFlowRegistryFill =
                getDeviceFlowRegistry().fill();
        /*
            callback, 最后执行完逻辑会调用ContextChainHolderImpl中的onMasterRoleAcquired, 状态INITIAL_FLOW_REGISTRY_FILL
         */
        Futures.addCallback(deviceFlowRegistryFill,
                new DeviceFlowRegistryCallback(deviceFlowRegistryFill, contextChainMastershipWatcher),
                MoreExecutors.directExecutor());
    }

    /*
        效果:
            1.创建transactionChainManager
            2.创建deviceFlowRegistry
            3.创建deviceGroupRegistry
            4.创建deviceMeterRegistry
     */
    @VisibleForTesting
    void lazyTransactionManagerInitialization() {
        if (!this.initialized.get()) {
            // 还没初始化
            if (LOG.isDebugEnabled()) {
                LOG.debug("Transaction chain manager for node {} created", deviceInfo);
            }
            // tx manager?
            this.transactionChainManager = new TransactionChainManager(dataBroker, deviceInfo.getNodeId().getValue());
            this.deviceFlowRegistry = new DeviceFlowRegistryImpl(deviceInfo.getVersion(), dataBroker,
                    deviceInfo.getNodeInstanceIdentifier());
            this.deviceGroupRegistry = new DeviceGroupRegistryImpl();
            this.deviceMeterRegistry = new DeviceMeterRegistryImpl();
        }

        transactionChainManager.activateTransactionManager();
        // 设置flag, 已经初始化完成
        initialized.set(true);
    }

    @Nullable
    @Override
    public <T> RequestContext<T> createRequestContext() {
        final Long xid = deviceInfo.reserveXidForDeviceMessage();

        final AbstractRequestContext<T> abstractRequestContext = new AbstractRequestContext<T>(xid) {
            @Override
            public void close() {
                requestContexts.remove(this);
            }
        };

        requestContexts.add(abstractRequestContext);
        return abstractRequestContext;
    }

    @Override
    public void onStateAcquired(final ContextChainState state) {
        hasState.set(true);
    }

    /*
        instantiateServiceInstance()方法最后回调
     */
    private class DeviceFlowRegistryCallback implements FutureCallback<List<com.google.common.base
            .Optional<FlowCapableNode>>> {
        private final ListenableFuture<List<com.google.common.base.Optional<FlowCapableNode>>> deviceFlowRegistryFill;
        private final ContextChainMastershipWatcher contextChainMastershipWatcher;

        DeviceFlowRegistryCallback(
                ListenableFuture<List<com.google.common.base.Optional<FlowCapableNode>>> deviceFlowRegistryFill,
                ContextChainMastershipWatcher contextChainMastershipWatcher) {
            this.deviceFlowRegistryFill = deviceFlowRegistryFill;
            this.contextChainMastershipWatcher = contextChainMastershipWatcher;
        }

        @Override
        public void onSuccess(@Nullable List<com.google.common.base.Optional<FlowCapableNode>> result) {
            if (LOG.isDebugEnabled()) {
                // Count all flows we read from datastore for debugging purposes.
                // This number do not always represent how many flows were actually added
                // to DeviceFlowRegistry, because of possible duplicates.
                // 统计从YANG读取出来的流
                long flowCount = Optional.ofNullable(result)
                        .map(Collections::singleton)
                        .orElse(Collections.emptySet())
                        .stream()
                        .flatMap(Collection::stream)
                        .filter(Objects::nonNull)
                        .flatMap(flowCapableNodeOptional -> flowCapableNodeOptional.asSet().stream())
                        .filter(Objects::nonNull)
                        .filter(flowCapableNode -> Objects.nonNull(flowCapableNode.getTable()))
                        .flatMap(flowCapableNode -> flowCapableNode.getTable().stream())
                        .filter(Objects::nonNull)
                        .filter(table -> Objects.nonNull(table.getFlow()))
                        .flatMap(table -> table.getFlow().stream())
                        .filter(Objects::nonNull)
                        .count();

                LOG.debug("Finished filling flow registry with {} flows for node: {}", flowCount, deviceInfo);
            }
            // 调用contextChainHolderImpl
            this.contextChainMastershipWatcher.onMasterRoleAcquired(deviceInfo, ContextChainMastershipState
                    .INITIAL_FLOW_REGISTRY_FILL);
        }

        @Override
        public void onFailure(Throwable throwable) {
            if (deviceFlowRegistryFill.isCancelled()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cancelled filling flow registry with flows for node: {}", deviceInfo);
                }
            } else {
                LOG.warn("Failed filling flow registry with flows for node: {} with exception: {}", deviceInfo,
                         throwable);
            }
            contextChainMastershipWatcher.onNotAbleToStartMastership(
                    deviceInfo,
                    "Was not able to fill flow registry on device",
                    false);
        }
    }

}
