/*
 * Copyright (c) 2016 Pantheon Technologies s.r.o. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.openflowplugin.impl.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opendaylight.mdsal.eos.binding.api.EntityOwnershipChange;
import org.opendaylight.mdsal.eos.binding.api.EntityOwnershipListenerRegistration;
import org.opendaylight.mdsal.eos.binding.api.EntityOwnershipService;
import org.opendaylight.mdsal.singleton.common.api.ClusterSingletonServiceProvider;
import org.opendaylight.openflowplugin.api.openflow.OFPManager;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionContext;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionStatus;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceContext;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceInfo;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceManager;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChain;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainHolder;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainMastershipState;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.MasterChecker;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.OwnershipChangeListener;
import org.opendaylight.openflowplugin.api.openflow.role.RoleContext;
import org.opendaylight.openflowplugin.api.openflow.role.RoleManager;
import org.opendaylight.openflowplugin.api.openflow.rpc.RpcContext;
import org.opendaylight.openflowplugin.api.openflow.rpc.RpcManager;
import org.opendaylight.openflowplugin.api.openflow.statistics.StatisticsContext;
import org.opendaylight.openflowplugin.api.openflow.statistics.StatisticsManager;
import org.opendaylight.openflowplugin.impl.util.DeviceStateUtil;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.FeaturesReply;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.mdsal.core.general.entity.rev150930.Entity;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.openflowplugin.rf.state.rev170713.ResultState;
import org.opendaylight.yangtools.yang.binding.KeyedInstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextChainHolderImpl implements ContextChainHolder, MasterChecker {
    private static final Logger LOG = LoggerFactory.getLogger(ContextChainHolderImpl.class);

    private static final String CONTEXT_CREATED_FOR_CONNECTION = " context created for connection: {}";
    private static final long REMOVE_DEVICE_FROM_DS_TIMEOUT = 5000L;
    private static final String ASYNC_SERVICE_ENTITY_TYPE = "org.opendaylight.mdsal.AsyncServiceCloseEntityType";

    private final Map<DeviceInfo, ContextChain> contextChainMap = new ConcurrentHashMap<>();
    private final Map<DeviceInfo, ? super ConnectionContext> connectingDevices = new ConcurrentHashMap<>();
    private final EntityOwnershipListenerRegistration eosListenerRegistration;
    private final ClusterSingletonServiceProvider singletonServiceProvider;
    private final ExecutorService executorService;
    private final OwnershipChangeListener ownershipChangeListener;
    private DeviceManager deviceManager;
    private RpcManager rpcManager;
    private StatisticsManager statisticsManager;
    private RoleManager roleManager;

    // OpenflowPluginProviderImpl中调用
    public ContextChainHolderImpl(final ExecutorService executorService,
                                  final ClusterSingletonServiceProvider singletonServiceProvider,
                                  final EntityOwnershipService entityOwnershipService,
                                  final OwnershipChangeListener ownershipChangeListener) {
        this.singletonServiceProvider = singletonServiceProvider;
        this.executorService = executorService;

        // ofp mastership
        this.ownershipChangeListener = ownershipChangeListener;
        this.ownershipChangeListener.setMasterChecker(this);

        // mdsal eos
        // 监听类型为："org.opendaylight.mdsal.AsyncServiceCloseEntityType"的EOS
        this.eosListenerRegistration = Objects
                .requireNonNull(entityOwnershipService.registerListener(ASYNC_SERVICE_ENTITY_TYPE, this));
    }

    @Override
    public <T extends OFPManager> void addManager(final T manager) {
        if (Objects.isNull(deviceManager) && manager instanceof DeviceManager) {
            LOG.trace("Context chain holder: Device manager OK.");
            deviceManager = (DeviceManager) manager;
        } else if (Objects.isNull(rpcManager) && manager instanceof RpcManager) {
            LOG.trace("Context chain holder: RPC manager OK.");
            rpcManager = (RpcManager) manager;
        } else if (Objects.isNull(statisticsManager) && manager instanceof StatisticsManager) {
            LOG.trace("Context chain holder: Statistics manager OK.");
            statisticsManager = (StatisticsManager) manager;
        } else if (Objects.isNull(roleManager) && manager instanceof RoleManager) {
            LOG.trace("Context chain holder: Role manager OK.");
            roleManager = (RoleManager) manager;
        }
    }

    /*
        理解：每个connection(sw连接)一个connectionContext

        为每个connection(switch)创建contextChain, 而每个contextChain是一个singleton service(即每个sw会有一个service运行在整个集群的一个节点上)
     */
    @VisibleForTesting
    void createContextChain(final ConnectionContext connectionContext) {
        // connectionContext信息是在HandshakeListenerImpl中设置(在connectionManagerImpl中创建)

        final DeviceInfo deviceInfo = connectionContext.getDeviceInfo();

        /*
            DeviceManager(在OpenflowPluginProvider中设置的),效果:
            1.设置ConnectionAdapterImpl中设置packetIn filter为true
            2.创建OutboundQueueHandler并设置到connectionAdapterImpl
            3.创建DeviceContextImpl
            4.创建OpenflowProtocolListenerFullImpl注册到ConnectionAdapterImpl对象,当收到消息回调OpenflowProtocolListenerFullImpl方法
         */
        final DeviceContext deviceContext = deviceManager.createContext(connectionContext);
        deviceContext.registerMastershipWatcher(this);
        LOG.debug("Device" + CONTEXT_CREATED_FOR_CONNECTION, deviceInfo);

        /*
            创建此connection的rpcContextImpl对象
         */
        final RpcContext rpcContext = rpcManager.createContext(deviceContext);
        rpcContext.registerMastershipWatcher(this);
        LOG.debug("RPC" + CONTEXT_CREATED_FOR_CONNECTION, deviceInfo);

        /*
            创建此connetion的statisticsContextImpl对象
         */
        final StatisticsContext statisticsContext = statisticsManager
                .createContext(deviceContext, ownershipChangeListener.isReconciliationFrameworkRegistered());
        statisticsContext.registerMastershipWatcher(this);
        LOG.debug("Statistics" + CONTEXT_CREATED_FOR_CONNECTION, deviceInfo);

        /*
            创建此connection的RoleContextImpl对象
                会创建SalRoleServiceImpl对象(包含roleService对象)
         */
        final RoleContext roleContext = roleManager.createContext(deviceContext);
        roleContext.registerMastershipWatcher(this);
        LOG.debug("Role" + CONTEXT_CREATED_FOR_CONNECTION, deviceInfo);

        /*
            1.创建ContextChainImpl对象
            2.设置deviceRemovedHandler
            3.添加contextImpl到索引
         */
        final ContextChain contextChain = new ContextChainImpl(this, connectionContext, executorService);
        contextChain.registerDeviceRemovedHandler(deviceManager);
        contextChain.registerDeviceRemovedHandler(rpcManager);
        contextChain.registerDeviceRemovedHandler(statisticsManager);
        contextChain.registerDeviceRemovedHandler(roleManager);
        contextChain.registerDeviceRemovedHandler(this);
        contextChain.addContext(deviceContext); //会context被装进GuardedContextImpl
        contextChain.addContext(rpcContext);
        contextChain.addContext(statisticsContext);
        contextChain.addContext(roleContext);
        // 索引contextChain
        contextChainMap.put(deviceInfo, contextChain);
        // 创建完contextChainImpl说明已经成功连上, 从connecting索引中去掉这设备
        connectingDevices.remove(deviceInfo);
        LOG.debug("Context chain" + CONTEXT_CREATED_FOR_CONNECTION, deviceInfo);

        // 效果是：设置ConnectionAdapterImpl中设置packetIn filter为false, 上面创建deviceContext开始时会设置值为true, 为了创建contextChain时不要接受PacketIn?
        deviceContext.onPublished();

        /*
            将contextChain注册到singleton service中,
            效果: 每个contextChain自身都是一个singleton service, 即每个switch connection都只会在cluster内一个节点上是主
                当选举出主时,会调用contextChain的instantiateServiceInstance()方法
         */
        contextChain.registerServices(singletonServiceProvider);
    }

    /*
        HandshakeListenerImpl中当handshake成功会调用
     */
    @Override
    public ConnectionStatus deviceConnected(final ConnectionContext connectionContext) throws Exception {
        final DeviceInfo deviceInfo = connectionContext.getDeviceInfo();
        final ContextChain contextChain = contextChainMap.get(deviceInfo);
        final FeaturesReply featuresReply = connectionContext.getFeatures();
        final Short auxiliaryId = featuresReply != null ? featuresReply.getAuxiliaryId() : null;

        // 辅助连接
        if (auxiliaryId != null && auxiliaryId != 0) {
            if (contextChain == null) {
                LOG.warn("An auxiliary connection for device {}, but no primary connection. Refusing connection.",
                         deviceInfo);
                return ConnectionStatus.REFUSING_AUXILIARY_CONNECTION;
            } else {
                if (contextChain.addAuxiliaryConnection(connectionContext)) {
                    LOG.info("An auxiliary connection was added to device: {}", deviceInfo);
                    return ConnectionStatus.MAY_CONTINUE;
                } else {
                    LOG.warn("Not able to add auxiliary connection to the device {}", deviceInfo);
                    return ConnectionStatus.REFUSING_AUXILIARY_CONNECTION;
                }
            }
        } else {
            LOG.info("Device {} connected.", deviceInfo);
            final boolean contextExists = contextChain != null;
            final boolean isClosing = contextExists && contextChain.isClosing();

            if (!isClosing && connectingDevices.putIfAbsent(deviceInfo, connectionContext) != null) {
                LOG.warn("Device {} is already trying to connect, wait until succeeded or disconnected.", deviceInfo);
                return ConnectionStatus.ALREADY_CONNECTED;
            }

            if (contextExists) {
                if (isClosing) {
                    LOG.warn("Device {} is already in termination state, closing all incoming connections.",
                             deviceInfo);
                    return ConnectionStatus.CLOSING;
                }

                LOG.warn("Device {} already connected. Closing previous connection", deviceInfo);
                destroyContextChain(deviceInfo);
                LOG.info("Old connection dropped, creating new context chain for device {}", deviceInfo);
                createContextChain(connectionContext);
            } else {
                // 设备第一次connect,需要创建contextChain
                LOG.info("No context chain found for device: {}, creating new.", deviceInfo);
                createContextChain(connectionContext);
            }

            return ConnectionStatus.MAY_CONTINUE;
        }

    }

    /*
        1.会在contextChainImpl中被调用,当contextChain在节点运行时,当前节点称为singleton master, 会尝试运行每个context的instantiateServiceInstance方法,
            运行出错就调用当前方法(调用父类onNotAbleToStartMastershipMandatory)且第三个参数为true.
          即在contextChain在当前节点成为master时, 尝试运行context的init方法, 出错则调用当前方法destroyContextChain
     */
    /*
        ContextChainMastershipWatcher interface:
        Event occurs if there was a try to acquire MASTER role.
        But it was not possible to start this MASTER role on device.
     */
    @Override
    public void onNotAbleToStartMastership(@Nonnull final DeviceInfo deviceInfo, @Nonnull final String reason,
                                           final boolean mandatory) {
        LOG.warn("Not able to set MASTER role on device {}, reason: {}", deviceInfo, reason);

        if (!mandatory) {
            return;
        }

        Optional.ofNullable(contextChainMap.get(deviceInfo)).ifPresent(contextChain -> {
            LOG.warn("This mastering is mandatory, destroying context chain and closing connection for device {}.",
                     deviceInfo);
            destroyContextChain(deviceInfo);
        });
    }

    /*
        ContextChainMastershipWatcher interface:
            Changed to MASTER role on device.成为设备的master

        调用情况: deviceContext, rpcContext, statisticsContext, roleContext执行完instantiateServiceInstance()后 //被ContextChain在某节点初始化时调用的

        deviceContext调用传入的状态是 INITIAL_FLOW_REGISTRY_FILL
        rpcContext是RPC_REGISTRATION
        statisticsContext是INITIAL_GATHERING, INITIAL_SUBMIT
        roleContext是MASTER_ON_DEVICE
     */
    @Override
    public void onMasterRoleAcquired(@Nonnull final DeviceInfo deviceInfo,
                                     @Nonnull final ContextChainMastershipState mastershipState) {
        Optional.ofNullable(contextChainMap.get(deviceInfo)).ifPresent(contextChain -> {

            /*
                当使用ReconciliationFramework时，在初始化context阶段不会收集数据，就会在这里匹配进入ReconciliationFramework
                    当ReconciliationFramework执行完成，回调时才开启收集数据continueInitializationAfterReconciliation
                    此时就会出现INITIAL_SUBMIT状态，不再进入ReconciliationFramework，转而执行下面else逻辑
             */
            if (ownershipChangeListener.isReconciliationFrameworkRegistered()
                    && !ContextChainMastershipState.INITIAL_SUBMIT.equals(mastershipState)) { //注意这里判断INITIAL_SUBMIT为了是在使用Framework情况下,先调用framework注册的服务，在下面reconciliationFrameworkCallback回调中才会重新调用持续收集数据，才会出现INITIAL_SUBMIT
                if (contextChain.isMastered(mastershipState, true)) { //使用reconciliationFramework情况下,在isMaster()中不会设状态
                    // 几个context(deviceContext,rpcContext,statisticContext,roleContext)都初始化完成(各自instantiateServiceInstance执行完成), 才会进入此处, 节点才能成为device的master

                    /*
                        如果开启reconciliationFramework, ownershipChangeListener会在reconciliationManagerImpl中有额外处理

                        reconciliationFrameworkCallback回调是收集statistics静态数据
                            理解：如果开了reconciliationFramework, 会在contextChain完全成为master后才收集
                     */
                    Futures.addCallback(ownershipChangeListener.becomeMasterBeforeSubmittedDS(deviceInfo), //钩子触发上层应用, 会调用ReconciliationManagerImpl.onDevicePrepared, 最终会调用注册到framework的service的startReconciliation方法
                                        reconciliationFrameworkCallback(deviceInfo, contextChain), // 回调reconciliationFramework, 收集数据, 会触发再次调用本方法并进入下面else逻辑
                                        MoreExecutors.directExecutor());
                }
            }
            /*
                进入此else if会设置WORKING_MASTER状态.有两种情况会进入:
                    1.如果不使用ReconciliationFramework, 在statisticsContext初始化就会直接向switch持续收集信息. 会出现状态INITIAL_SUBMMIT
                    2.使用ReconciliationFramework, 在statisticsContext初始化时不会开启数据收集，会进入上面if判断，进入reconciliationFramework，但是不会设置WORKING_MASTER状态. 只有再最后回调reconciliationFrameworkCallback
                        时, 才会重新调用statisticsContext开启持续收集信息，这是就会出现状态INITIAL_SUBMMIT，就会进入下面处理，即设置WORKING_MASTER状态，同时出发原生注册mastership的应用
             */
            else if (contextChain.isMastered(mastershipState, false)) { //不使用reconciliationFramework情况下,在isMastered()方法内会直接设置contextChain及各个context状态为WORKING_MASTER
                /*
                    几个context(deviceContext,rpcContext,statisticContext,roleContext)都初始化完成(各自instantiateServiceInstance执行完成), 才会进入此处, 节点才能成为device的master

                    到达此处, contextChain及各个context状态已被设置为WORKING_MASTER
                 */
                LOG.info("Role MASTER was granted to device {}", deviceInfo);
                // 钩子触发上层应用(不使用ReconciliationFramework情况下的钩子)
                ownershipChangeListener.becomeMaster(deviceInfo);

                // TODO:
                deviceManager.sendNodeAddedNotification(deviceInfo.getNodeInstanceIdentifier());
            }
        });
    }

    /*
        ContextChainMastershipWatcher interface:
        Change to SLAVE role on device was successful.
     */
    @Override
    public void onSlaveRoleAcquired(final DeviceInfo deviceInfo) {
        ownershipChangeListener.becomeSlaveOrDisconnect(deviceInfo);
        LOG.info("Role SLAVE was granted to device {}", deviceInfo);
        Optional.ofNullable(contextChainMap.get(deviceInfo)).ifPresent(ContextChain::makeContextChainStateSlave);
    }

    /*
        ContextChainMastershipWatcher interface:
        Change to SLAVE role on device was not able.
     */
    @Override
    public void onSlaveRoleNotAcquired(final DeviceInfo deviceInfo, final String reason) {
        LOG.warn("Not able to set SLAVE role on device {}, reason: {}", deviceInfo, reason);
        Optional.ofNullable(contextChainMap.get(deviceInfo)).ifPresent(contextChain -> destroyContextChain(deviceInfo));
    }

    /*
        触发contextChainImpl的singleton service关闭等过程的入口, 底层connectionContextImpl调用
     */
    @Override
    public void onDeviceDisconnected(final ConnectionContext connectionContext) {
        final DeviceInfo deviceInfo = connectionContext.getDeviceInfo();

        Optional.ofNullable(connectionContext.getDeviceInfo()).map(contextChainMap::get).ifPresent(contextChain -> {
            if (contextChain.auxiliaryConnectionDropped(connectionContext)) {
                LOG.info("Auxiliary connection from device {} disconnected.", deviceInfo);
            } else {
                LOG.info("Device {} disconnected.", deviceInfo);
                destroyContextChain(deviceInfo);
            }
        });
    }

    @VisibleForTesting
    boolean checkAllManagers() {
        return Objects.nonNull(deviceManager) && Objects.nonNull(rpcManager) && Objects.nonNull(statisticsManager)
                && Objects.nonNull(roleManager);
    }

    @Override
    public ContextChain getContextChain(final DeviceInfo deviceInfo) {
        return contextChainMap.get(deviceInfo);
    }

    @Override
    public void close() throws Exception {
        Map<DeviceInfo, ContextChain> copyOfChains = new HashMap<>(contextChainMap);
        copyOfChains.keySet().forEach(this::destroyContextChain);
        copyOfChains.clear();
        contextChainMap.clear();
        eosListenerRegistration.close();
    }

    /*
        mdsal的EntityOwnershipListener interface, 这里监听EOS状态变化:
        因为底层contextChainImpl是一个singleton service, 而singleton service底层是EOS,
            且会为每个singleton service注册"org.opendaylight.mdsal.AsyncServiceCloseEntityType"类型的entity, 如果不是owner了会触发此type事件

        监听类型为："org.opendaylight.mdsal.AsyncServiceCloseEntityType"的entity 状态变化, 它是每个singleton service都会创建的一个entity type

        捕获到此类型的entityOwnershipChange, 且没有owner了, 触发后续:
            1.发送notification通知inventory树要删除node节点
            2.除inventory yang树的node节点
     */
    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    public void ownershipChanged(EntityOwnershipChange entityOwnershipChange) {
        if (entityOwnershipChange.getState().hasOwner()) {
            return;
        }

        // Findbugs flags a false violation for "Unchecked/unconfirmed cast" from GenericEntity to Entity hence the
        // suppression above. The suppression is temporary until EntityOwnershipChange is modified to eliminate the
        // violation.
        /*
            contextChainImpl是一个singleton service.
            获取entityName是device id
         */
        final String entityName = entityOwnershipChange
                .getEntity()
                .getIdentifier()
                .firstKeyOf(Entity.class)
                .getName();

        if (Objects.nonNull(entityName)) {
            LOG.debug("Entity {} has no owner", entityName);
            try {
                //TODO:Remove notifications
                final KeyedInstanceIdentifier<Node, NodeKey> nodeInstanceIdentifier =
                        DeviceStateUtil.createNodeInstanceIdentifier(new NodeId(entityName));
                // 发送notification通知inventory树要删除节点
                deviceManager.sendNodeRemovedNotification(nodeInstanceIdentifier);

                LOG.info("Try to remove device {} from operational DS", entityName);
                // 删除inventory yang树的node节点
                deviceManager.removeDeviceFromOperationalDS(nodeInstanceIdentifier)
                        .get(REMOVE_DEVICE_FROM_DS_TIMEOUT, TimeUnit.MILLISECONDS);
                LOG.info("Removing device from operational DS {} was successful", entityName);
            } catch (TimeoutException | ExecutionException | NullPointerException | InterruptedException e) {
                LOG.warn("Not able to remove device {} from operational DS. ", entityName, e);
            }
        }
    }

    /*
        效果: becomeSlave或者disconnect

        上层应用调整switch master所在街道调用此方法即可
     */
    private void destroyContextChain(final DeviceInfo deviceInfo) {
        // 通知注册到mastershipService的应用(原生/reconciliationFramework)
        ownershipChangeListener.becomeSlaveOrDisconnect(deviceInfo);
        Optional.ofNullable(contextChainMap.get(deviceInfo)).ifPresent(contextChain -> {
            // 发送device删除inventory的通知
            deviceManager.sendNodeRemovedNotification(deviceInfo.getNodeInstanceIdentifier());
            /*
                调用ContextChainImpl的close方法, 作用: 会回收/关闭switch在ofp层次相关的所有对象, 包括singleton service contextChain的关闭, 各个context的关闭等
             */
            contextChain.close();
        });
    }

    @Override
    public List<DeviceInfo> listOfMasteredDevices() {
        return contextChainMap.entrySet().stream()
                .filter(deviceInfoContextChainEntry -> deviceInfoContextChainEntry.getValue()
                        .isMastered(ContextChainMastershipState.CHECK, false)).map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isAnyDeviceMastered() {
        return contextChainMap.entrySet().stream().findAny()
                .filter(deviceInfoContextChainEntry -> deviceInfoContextChainEntry.getValue()
                        .isMastered(ContextChainMastershipState.CHECK, false)).isPresent();
    }

    @Override
    public void onDeviceRemoved(final DeviceInfo deviceInfo) {
        contextChainMap.remove(deviceInfo);
        LOG.debug("Context chain removed for node {}", deviceInfo);
    }

    private FutureCallback<ResultState> reconciliationFrameworkCallback(@Nonnull DeviceInfo deviceInfo,
                                                                        ContextChain contextChain) {
        return new FutureCallback<ResultState>() {
            @Override
            public void onSuccess(@Nullable ResultState result) {
                if (ResultState.DONOTHING == result) {
                    LOG.info("Device {} connection is enabled by reconciliation framework.", deviceInfo);
                    /*
                        调用statisticsContextImpl收集数据.

                        在statisticsContextImpl中如果没使用reconciliationFramework. statisticsContextImpl初始化实例就会收集.
                        理解：如果开了reconciliationFramework, 会在contextChain完全成为master后才收集
                     */
                    //注意：在reconciliationFramework处理完，再调用此方法，后续会再次调用到上面的 onMasterRoleAcquired且状态为INIT_SUBMIT, 在这个情况下就会进入else if设置状态为WORKING_MASTER
                    contextChain.continueInitializationAfterReconciliation();
                } else {
                    LOG.warn("Reconciliation framework failure for device {}", deviceInfo);
                    destroyContextChain(deviceInfo);
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable throwable) {
                LOG.warn("Reconciliation framework failure.");
                destroyContextChain(deviceInfo);
            }
        };
    }
}
