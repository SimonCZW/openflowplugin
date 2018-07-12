/*
 * Copyright (c) 2013 Pantheon Technologies s.r.o. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */


package org.opendaylight.openflowjava.protocol.impl.core;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import org.opendaylight.openflowjava.protocol.api.connection.ConnectionConfiguration;
import org.opendaylight.openflowjava.protocol.api.connection.SwitchConnectionHandler;
import org.opendaylight.openflowjava.protocol.api.extensibility.DeserializerRegistry;
import org.opendaylight.openflowjava.protocol.api.extensibility.OFDeserializer;
import org.opendaylight.openflowjava.protocol.api.extensibility.OFGeneralDeserializer;
import org.opendaylight.openflowjava.protocol.api.extensibility.OFGeneralSerializer;
import org.opendaylight.openflowjava.protocol.api.extensibility.OFSerializer;
import org.opendaylight.openflowjava.protocol.api.extensibility.SerializerRegistry;
import org.opendaylight.openflowjava.protocol.api.keys.ActionSerializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.ExperimenterActionDeserializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.ExperimenterDeserializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.ExperimenterIdDeserializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.ExperimenterIdMeterSubTypeSerializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.ExperimenterIdSerializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.ExperimenterInstructionDeserializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.ExperimenterSerializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.InstructionSerializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.MatchEntryDeserializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.MatchEntrySerializerKey;
import org.opendaylight.openflowjava.protocol.api.keys.MessageCodeKey;
import org.opendaylight.openflowjava.protocol.api.keys.MessageTypeKey;
import org.opendaylight.openflowjava.protocol.api.keys.TypeToClassKey;
import org.opendaylight.openflowjava.protocol.impl.deserialization.DeserializationFactory;
import org.opendaylight.openflowjava.protocol.impl.deserialization.DeserializerRegistryImpl;
import org.opendaylight.openflowjava.protocol.impl.serialization.SerializationFactory;
import org.opendaylight.openflowjava.protocol.impl.serialization.SerializerRegistryImpl;
import org.opendaylight.openflowjava.protocol.spi.connection.SwitchConnectionProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.config.rev140630.TransportProtocol;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.oxm.rev150225.MatchField;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.oxm.rev150225.OxmClassBase;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.ErrorMessage;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.experimenter.core.ExperimenterDataOfChoice;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.meter.band.header.meter.band.MeterBandExperimenterCase;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.queue.property.header.QueueProperty;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.table.features.properties.grouping.TableFeatureProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposed class for server handling. <br>
 * C - {@link MatchEntrySerializerKey} parameter representing oxm_class (see specification)<br>
 * F - {@link MatchEntrySerializerKey} parameter representing oxm_field (see specification)
 * @author mirehak
 * @author michal.polkorab
 */
public class SwitchConnectionProviderImpl implements SwitchConnectionProvider, ConnectionInitializer {

    private static final Logger LOG = LoggerFactory
            .getLogger(SwitchConnectionProviderImpl.class);
    private SwitchConnectionHandler switchConnectionHandler;
    private ServerFacade serverFacade;
    private final ConnectionConfiguration connConfig;
    private final SerializationFactory serializationFactory;
    private final SerializerRegistry serializerRegistry;
    private final DeserializerRegistry deserializerRegistry;
    private final DeserializationFactory deserializationFactory;
    private TcpConnectionInitializer connectionInitializer;

    /*
       在openflowjava.xml创建了SwitchConnectionProvider，注册为service等其他bundle在blueprint中调用
    */
    public SwitchConnectionProviderImpl(ConnectionConfiguration connConfig) {
        this.connConfig = connConfig;
        serializerRegistry = new SerializerRegistryImpl();
        if (connConfig != null) {
            serializerRegistry.setGroupAddModConfig(connConfig.isGroupAddModEnabled());
        }
        serializerRegistry.init();
        serializationFactory = new SerializationFactory(serializerRegistry);
        deserializerRegistry = new DeserializerRegistryImpl();
        deserializerRegistry.init();
        deserializationFactory = new DeserializationFactory(deserializerRegistry);
    }

    /*
        在OpenflowPluginProviderImpl的startSwitchConnections()中被调用 (是在OpenflowPluginProvider创建的调用链中调用)
        传入的switchConnectionHandler是：ConnectionManagerImpl
     */
    @Override
    public void setSwitchConnectionHandler(final SwitchConnectionHandler switchConnectionHandler) {
        LOG.debug("setSwitchConnectionHandler");
        this.switchConnectionHandler = switchConnectionHandler;
    }

    @Override
    public ListenableFuture<Boolean> shutdown() {
        LOG.debug("Shutdown summoned");
        if (serverFacade == null) {
            LOG.warn("Can not shutdown - not configured or started");
            throw new IllegalStateException("SwitchConnectionProvider is not started or not configured.");
        }
        return serverFacade.shutdown();
    }

    /*
        在OpenflowPluginProviderImpl的startSwitchConnections()中被调用 (是在OpenflowPluginProvider创建的调用链中调用)
     */
    @Override
    @SuppressWarnings("checkstyle:IllegalCatch")
    public ListenableFuture<Boolean> startup() {
        LOG.debug("Startup summoned");
        ListenableFuture<Boolean> result = null;
        try {
            /*
                效果: 创建tcp/udp的server监听6633、6653端口
                    1.创建TcpServer，后面最终会调用其run()方法
                    2.当有连接时，调用TcpChannelInitializer的initChannel()方法，会调用调用openflowplugin的ConnectionManagerImpl
             */
            serverFacade = createAndConfigureServer();
            if (switchConnectionHandler == null) {
                throw new IllegalStateException("SwitchConnectionHandler is not set");
            }
            // 创建一个线程运行ServerFacade
            new Thread(serverFacade).start();
            result = serverFacade.getIsOnlineFuture();
        } catch (RuntimeException e) {
            final SettableFuture<Boolean> exResult = SettableFuture.create();
            exResult.setException(e);
            result = exResult;
        }
        return result;
    }

    /*
        创建tcp/udp的server监听6633、6653端口
            1.创建TcpServer，后面最终会调用其run()方法
            2.当有连接时，调用TcpChannelInitializer的initChannel()方法，会调用调用openflowplugin的ConnectionManagerImpl
     */
    private ServerFacade createAndConfigureServer() {
        LOG.debug("Configuring ..");
        ServerFacade server = null;
        // 创建channel factory并配置
        final ChannelInitializerFactory factory = new ChannelInitializerFactory();
        //设置ConnectionManagerImpl: 应该是这里当有switch连上了就会触发ConnectionManagerImpl的onSwitchConnected()方法
        factory.setSwitchConnectionHandler(switchConnectionHandler);
        factory.setSwitchIdleTimeout(connConfig.getSwitchIdleTimeout());
        factory.setTlsConfig(connConfig.getTlsConfiguration());
        factory.setSerializationFactory(serializationFactory);
        factory.setDeserializationFactory(deserializationFactory);
        factory.setUseBarrier(connConfig.useBarrier());
        factory.setChannelOutboundQueueSize(connConfig.getChannelOutboundQueueSize());
        final TransportProtocol transportProtocol = (TransportProtocol) connConfig.getTransferProtocol();

        // Check if Epoll native transport is available.
        // TODO : Add option to disable Epoll.
        boolean isEpollEnabled = Epoll.isAvailable();

        if (TransportProtocol.TCP.equals(transportProtocol) || TransportProtocol.TLS.equals(transportProtocol)) {
            // 创建TCP handler用于监听地址及端口
            server = new TcpHandler(connConfig.getAddress(), connConfig.getPort());
            // 创建TCP publishing channel initializer: 这里创建的initializer是有ConnectionManagerImpl的的引用
            final TcpChannelInitializer channelInitializer = factory.createPublishingChannelInitializer();
            ((TcpHandler) server).setChannelInitializer(channelInitializer);
            // 会创建底层netty相关的channel对象
            ((TcpHandler) server).initiateEventLoopGroups(connConfig.getThreadConfiguration(), isEpollEnabled);

            // 获取上一步创建的netty底层channel相关对象：EventLoopGroup
            final EventLoopGroup workerGroupFromTcpHandler = ((TcpHandler) server).getWorkerGroup();
            // 创建TcpConnectionInitializer
            connectionInitializer = new TcpConnectionInitializer(workerGroupFromTcpHandler, isEpollEnabled);
            // 设置上面的channelInitializer(带有ConnectionManagerImpl引用)
            connectionInitializer.setChannelInitializer(channelInitializer);
            /*
             会调用TcpHandler run()方法, 最终效果会监听tcp端口
                TcpHandler run()中设置了再调用到TcpChannelInitializer的initChannel()方法;
                当远程sw连上就会调用initChannel()，其会调用openflowplugin的ConnectionManagerImpl
             */
            connectionInitializer.run();
        } else if (TransportProtocol.UDP.equals(transportProtocol)) {
            server = new UdpHandler(connConfig.getAddress(), connConfig.getPort());
            ((UdpHandler) server).initiateEventLoopGroups(connConfig.getThreadConfiguration(), isEpollEnabled);
            ((UdpHandler) server).setChannelInitializer(factory.createUdpChannelInitializer());
        } else {
            throw new IllegalStateException("Unknown transport protocol received: " + transportProtocol);
        }
        server.setThreadConfig(connConfig.getThreadConfiguration());
        return server;
    }

    public ServerFacade getServerFacade() {
        return serverFacade;
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }

    @Override
    public boolean unregisterSerializer(final ExperimenterSerializerKey key) {
        return serializerRegistry.unregisterSerializer((MessageTypeKey<?>) key);
    }

    @Override
    public boolean unregisterDeserializer(final ExperimenterDeserializerKey key) {
        return deserializerRegistry.unregisterDeserializer((MessageCodeKey) key);
    }

    // 在 org.opendaylight.openflowjava.nx.NiciraExtensionCodecRegistratorImpl造器中会调用注册serializer
    // 以及在 NiciraExtensionsRegistrator中注册
    @Override
    public void registerActionSerializer(final ActionSerializerKey<?> key,
            final OFGeneralSerializer serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void registerActionDeserializer(final ExperimenterActionDeserializerKey key,
            final OFGeneralDeserializer deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerInstructionSerializer(final InstructionSerializerKey<?> key,
            final OFGeneralSerializer serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void registerInstructionDeserializer(final ExperimenterInstructionDeserializerKey key,
            final OFGeneralDeserializer deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public <C extends OxmClassBase, F extends MatchField> void registerMatchEntrySerializer(
            final MatchEntrySerializerKey<C, F> key, final OFGeneralSerializer serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void registerMatchEntryDeserializer(final MatchEntryDeserializerKey key,
            final OFGeneralDeserializer deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerErrorDeserializer(final ExperimenterIdDeserializerKey key,
            final OFDeserializer<ErrorMessage> deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerExperimenterMessageDeserializer(ExperimenterIdDeserializerKey key,
            OFDeserializer<? extends ExperimenterDataOfChoice> deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerMultipartReplyMessageDeserializer(ExperimenterIdDeserializerKey key,
            OFDeserializer<? extends ExperimenterDataOfChoice> deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerMultipartReplyTFDeserializer(final ExperimenterIdDeserializerKey key,
            final OFGeneralDeserializer deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerQueuePropertyDeserializer(final ExperimenterIdDeserializerKey key,
            final OFDeserializer<QueueProperty> deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerMeterBandDeserializer(final ExperimenterIdDeserializerKey key,
            final OFDeserializer<MeterBandExperimenterCase> deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerExperimenterMessageSerializer(
            ExperimenterIdSerializerKey<? extends ExperimenterDataOfChoice> key,
            OFSerializer<? extends ExperimenterDataOfChoice> serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void registerMultipartRequestSerializer(ExperimenterIdSerializerKey<? extends ExperimenterDataOfChoice> key,
                                                   OFSerializer<? extends ExperimenterDataOfChoice> serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void registerMultipartRequestTFSerializer(final ExperimenterIdSerializerKey<TableFeatureProperties> key,
            final OFGeneralSerializer serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    /**
     * Deprecated.
     *
     * @deprecated Since we have used ExperimenterIdMeterSubTypeSerializerKey as MeterBandSerializer's key, in order
     *     to avoid the occurrence of an error, we should discard this function.
     */
    @Override
    @Deprecated
    public void registerMeterBandSerializer(final ExperimenterIdSerializerKey<MeterBandExperimenterCase> key,
            final OFSerializer<MeterBandExperimenterCase> serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void registerMeterBandSerializer(
            final ExperimenterIdMeterSubTypeSerializerKey<MeterBandExperimenterCase> key,
            final OFSerializer<MeterBandExperimenterCase> serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void initiateConnection(final String host, final int port) {
        connectionInitializer.initiateConnection(host, port);
    }

    @Override
    public ConnectionConfiguration getConfiguration() {
        return this.connConfig;
    }

    @Override
    public <K> void registerSerializer(MessageTypeKey<K> key, OFGeneralSerializer serializer) {
        serializerRegistry.registerSerializer(key, serializer);
    }

    @Override
    public void registerDeserializer(MessageCodeKey key, OFGeneralDeserializer deserializer) {
        deserializerRegistry.registerDeserializer(key, deserializer);
    }

    @Override
    public void registerDeserializerMapping(final TypeToClassKey key, final Class<?> clazz) {
        deserializationFactory.registerMapping(key, clazz);
    }

    @Override
    public boolean unregisterDeserializerMapping(final TypeToClassKey key) {
        return deserializationFactory.unregisterMapping(key);
    }
}
