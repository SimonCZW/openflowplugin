/*
 * Copyright (c) 2016 Pantheon Technologies s.r.o. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.openflowplugin.impl.lifecycle;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.opendaylight.mdsal.singleton.common.api.ClusterSingletonServiceProvider;
import org.opendaylight.mdsal.singleton.common.api.ServiceGroupIdentifier;
import org.opendaylight.openflowplugin.api.openflow.OFPContext;
import org.opendaylight.openflowplugin.api.openflow.connection.ConnectionContext;
import org.opendaylight.openflowplugin.api.openflow.device.DeviceInfo;
import org.opendaylight.openflowplugin.api.openflow.device.handlers.DeviceRemovedHandler;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChain;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainMastershipState;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainMastershipWatcher;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainState;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ContextChainStateListener;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.GuardedContext;
import org.opendaylight.openflowplugin.api.openflow.lifecycle.ReconciliationFrameworkStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextChainImpl implements ContextChain {
    private static final Logger LOG = LoggerFactory.getLogger(ContextChainImpl.class);

    private final AtomicBoolean masterStateOnDevice = new AtomicBoolean(false);
    private final AtomicBoolean initialGathering = new AtomicBoolean(false);
    private final AtomicBoolean initialSubmitting = new AtomicBoolean(false);
    private final AtomicBoolean registryFilling = new AtomicBoolean(false);
    private final AtomicBoolean rpcRegistration = new AtomicBoolean(false);
    private final List<DeviceRemovedHandler> deviceRemovedHandlers = new CopyOnWriteArrayList<>();
    private final List<GuardedContext> contexts = new CopyOnWriteArrayList<>();
    private final List<ConnectionContext> auxiliaryConnections = new CopyOnWriteArrayList<>();
    private final ExecutorService executorService;
    private final ContextChainMastershipWatcher contextChainMastershipWatcher;
    private final DeviceInfo deviceInfo;
    private final ConnectionContext primaryConnection;
    private final AtomicReference<ContextChainState> contextChainState =
            new AtomicReference<>(ContextChainState.UNDEFINED);
    private AutoCloseable registration;

    ContextChainImpl(@Nonnull final ContextChainMastershipWatcher contextChainMastershipWatcher,
                     @Nonnull final ConnectionContext connectionContext,
                     @Nonnull final ExecutorService executorService) {
        this.contextChainMastershipWatcher = contextChainMastershipWatcher; //是contextChainHolderImpl
        this.primaryConnection = connectionContext;
        this.deviceInfo = connectionContext.getDeviceInfo();
        this.executorService = executorService;
    }

    /*
        在contextChainHolderImpl创建当前对象后,会调用.添加deviceContext, rpcContext, statisticsContext, roleContext
     */
    @Override
    public <T extends OFPContext> void addContext(@Nonnull final T context) {
        contexts.add(new GuardedContextImpl(context));
    }

    /*
        connection(switch)与控制器handshake完成后,为每个connection(switch)创建一个contextChainImpl(自身), 最后将每个contextChain注册为一个singleton service
            当选举出singleton service在一个节点上为master，就会运行当前方法.
        即每个connection(switch)会在一个节点上有一个contextChain作为singleton service运行

        此方法效果:
            1.运行contexts中每个context对象的instantiateServiceInstance()方法
     */
    @Override
    @SuppressWarnings("checkstyle:IllegalCatch")
    public void instantiateServiceInstance() {
        try {
            /*
                运行contexts中每个context对象的instantiateServiceInstance()方法. context是在创建当前对象时传入的deviceContext, rpcContext, statisticsContext, roleContext
                实际效果:
                    1.deviceContext.instantiateServiceInstance(): 将ovs/nc写入operational yang; 收集switch信息;
                    2.rpcContext.instantiateServiceInstance(): 创建各个of sal service并注册rpcImplement到rpcContext
                    3.statisticsContext.instantiateServiceInstance(): 填写需要收集信息类型; 收集dynamic数据;
                    4.roleContext.instantiateServiceInstance(): 通知device成为master, rpc set-role

                初始化各个context, 它们最终都会回调ContextChainHolderImpl的onMasterRoleAcquired()方法, 只有当下面几个context都初始化成功, 才会让当前节点成为switch(device)的master
                并且发送消息到设备通知.

                即实际上odl openflowplugin对switch的mastership选举, 是基于odl singleton service的选举
             */
            contexts.forEach(OFPContext::instantiateServiceInstance);
            LOG.info("Started clustering services for node {}", deviceInfo);
        } catch (final Exception ex) {
            LOG.warn("Not able to start clustering services for node {}", deviceInfo);
            /*
                如果运行出错, 分配一个线程运行 contextChainHolderImpl的onNotAbleToStartMastershipMandatory方法(是一个父类方法),
                    实际会调用onNotAbleToStartMastership 第三个参数为true
                效果: 会destroyContextChain, 可能slave或者disconnection
             */
            executorService.execute(() -> contextChainMastershipWatcher
                    .onNotAbleToStartMastershipMandatory(deviceInfo, ex.toString()));
        }
    }

    /*
        singleton service关闭时会调用. 在本类中触发的位置是：close()方法中的`registration.close()`
     */
    @Override
    public ListenableFuture<Void> closeServiceInstance() {

        // 调用ContextChainHolderImpl.onSlaveRoleAcquired, 效果是触发注册了mastershipService的上层应用
        contextChainMastershipWatcher.onSlaveRoleAcquired(deviceInfo);

        /*
            调用各个context的closeServiceInstance方法
                device: 关闭transactionChainManager
                rpc: unregister rpc
                statistics: 关闭数据收集
                role: changeLastRoleFuture
         */
        final ListenableFuture<List<Void>> servicesToBeClosed = Futures
                .allAsList(Lists.reverse(contexts)
                        .stream()
                        .map(OFPContext::closeServiceInstance)
                        .collect(Collectors.toList()));

        return Futures.transform(servicesToBeClosed, (input) -> {
            LOG.info("Closed clustering services for node {}", deviceInfo);
            return null;
        }, executorService);
    }

    @Nonnull
    @Override
    public ServiceGroupIdentifier getIdentifier() {
        return deviceInfo.getServiceIdentifier();
    }

    /*
        ContextChainHolderImpl.destroyContextChain()中调用
     */
    @Override
    @SuppressWarnings("checkstyle:IllegalCatch")
    public void close() {
        if (ContextChainState.CLOSED.equals(contextChainState.get())) {
            LOG.debug("ContextChain for node {} is already in TERMINATION state.", deviceInfo);
            return;
        }

        // 设置状态CLOSED
        contextChainState.set(ContextChainState.CLOSED);
        /*
            设置各个阶段master状态为false:
                registryFilling.set(false);
                initialSubmitting.set(false);
                initialGathering.set(false);
                masterStateOnDevice.set(false);
                rpcRegistration.set(false);
         */
        unMasterMe();

        // 关闭辅助连接
        // Close all connections to devices
        auxiliaryConnections.forEach(connectionContext -> connectionContext.closeConnection(false));
        auxiliaryConnections.clear();

        // If we are still registered and we are not already closing, then close the registration
        if (Objects.nonNull(registration)) {
            try {
                /*
                    此registeration是registerServices()方法中注册为singleton service的返回
                            registration = Objects.requireNonNull(clusterSingletonServiceProvider
                                                .registerClusterSingletonService(this));

                    结合mdsal源码此变量是对象：AbstractClusterSingletonServiceRegistration

                    最终会调用自身的方法this.closeServiceInstance()方法
                 */
                registration.close();
                registration = null;
                LOG.info("Closed clustering services registration for node {}", deviceInfo);
            } catch (final Exception e) {
                LOG.warn("Failed to close clustering services registration for node {} with exception: ",
                        deviceInfo, e);
            }
        }


        /*
            调用所有context的close()方法: device/statistics/rpc/role
            会关闭/回收各个资源对象
         */
        // Close all contexts (device, statistics, rpc)
        contexts.forEach(OFPContext::close);
        contexts.clear();

        /*
            调用各个manager.onDeviceRemoved方法: 作用都是删除manager中此device的context索引
            DeviceManagerImpl
            RpcManagerImpl
            StatisticsManagerImpl
            RoleManagerImpl
            ContextChainHolderImpl
         */
        // We are closing, so cleanup all managers now
        deviceRemovedHandlers.forEach(h -> h.onDeviceRemoved(deviceInfo));
        deviceRemovedHandlers.clear();

        // 保证关闭ConnectionContextImpl会调用ConnectionContextImpl.disconnectDevice(false, true) 回收handshakeContext等connection资源
        primaryConnection.closeConnection(false);

    }

    @Override
    public void makeContextChainStateSlave() {
        unMasterMe();
        changeMastershipState(ContextChainState.WORKING_SLAVE);
    }

    /*
        在contextChainHolder中创建contextChain时会调用, 将contextChainImpl自身注册到ClusterSingletonService
     */
    @Override
    public void registerServices(final ClusterSingletonServiceProvider clusterSingletonServiceProvider) {
        registration = Objects.requireNonNull(clusterSingletonServiceProvider
                .registerClusterSingletonService(this));
        LOG.debug("Registered clustering services for node {}", deviceInfo);
    }

    @Override
    public boolean isMastered(@Nonnull ContextChainMastershipState mastershipState,
                              boolean inReconciliationFrameworkStep) {
        switch (mastershipState) {
            case INITIAL_SUBMIT: // statisticsContext
                LOG.debug("Device {}, initial submit OK.", deviceInfo);
                this.initialSubmitting.set(true);
                break;
            case MASTER_ON_DEVICE: // roleContext
                LOG.debug("Device {}, master state OK.", deviceInfo);
                this.masterStateOnDevice.set(true);
                break;
            case INITIAL_GATHERING: // statisticsContext
                LOG.debug("Device {}, initial gathering OK.", deviceInfo);
                this.initialGathering.set(true);
                break;
            case RPC_REGISTRATION: // rpcContext
                LOG.debug("Device {}, RPC registration OK.", deviceInfo);
                this.rpcRegistration.set(true);
                break;
            case INITIAL_FLOW_REGISTRY_FILL: // 是deviceContext.instantiateServiceInstance执行完传入状态
                // Flow registry fill is not mandatory to work as a master
                LOG.debug("Device {}, initial registry filling OK.", deviceInfo);
                this.registryFilling.set(true);
                break;
            case CHECK:
                // no operation
                break;
            default:
                // no operation
                break;
        }

        /*
            几个context(deviceContext,rpcContext,statisticContext,roleContext)都初始化完成(各自instantiateServiceInstance执行完成), result才会true

            ReconciliationFramework和initialSubmitting是互斥关系, 使用ReconciliationFramework,initialSubmitting就是false(可以看statisticsContextImpl中逻辑)
         */
        final boolean result = initialGathering.get() && masterStateOnDevice.get() && rpcRegistration.get()
                && inReconciliationFrameworkStep || initialSubmitting.get();

        if (!inReconciliationFrameworkStep && result && mastershipState != ContextChainMastershipState.CHECK) {
            // 不使用ReconciliationFramework情况下, deviceContext,rpcContext,statisticContext,roleContext都初始化完成后,就会进入此
            LOG.info("Device {} is able to work as master{}", deviceInfo,
                     registryFilling.get() ? "." : " WITHOUT flow registry !!!");
            /*
                1.设置contextChainState状态为WORKING_MASTER
                2.设置各个context(deviceContext,rpcContext,statisticContext,roleContext)状态为WORKING_MASTER
             */
            changeMastershipState(ContextChainState.WORKING_MASTER);
        }

        return result;
    }

    @Override
    public boolean isClosing() {
        return ContextChainState.CLOSED.equals(contextChainState.get());
    }

    @Override
    public void continueInitializationAfterReconciliation() {
        contexts.forEach(context -> {
            if (context.map(ReconciliationFrameworkStep.class::isInstance)) {
                context.map(ReconciliationFrameworkStep.class::cast).continueInitializationAfterReconciliation();
            }
        });
    }

    @Override
    public boolean addAuxiliaryConnection(@Nonnull ConnectionContext connectionContext) {
        return connectionContext.getFeatures().getAuxiliaryId() != 0
                && !ConnectionContext.CONNECTION_STATE.RIP.equals(primaryConnection.getConnectionState())
                && auxiliaryConnections.add(connectionContext);
    }

    @Override
    public boolean auxiliaryConnectionDropped(@Nonnull ConnectionContext connectionContext) {
        return auxiliaryConnections.remove(connectionContext);
    }

    @Override
    public void registerDeviceRemovedHandler(@Nonnull final DeviceRemovedHandler deviceRemovedHandler) {
        deviceRemovedHandlers.add(deviceRemovedHandler);
    }

    /*
        给各个context设置新状态
     */
    private void changeMastershipState(final ContextChainState newContextChainState) {
        if (ContextChainState.CLOSED.equals(this.contextChainState.get())) {
            return;
        }

        // 新connection创建的contextChainImpl 默认是UNDEFINED
        boolean propagate = ContextChainState.UNDEFINED.equals(this.contextChainState.get());

        // 设置为新
        this.contextChainState.set(newContextChainState);

        if (propagate) {
            // 新connection创将的contextChainImpl, 第一次调用changeMastershipState此方法, 无论设置为slave / master均会执行下面操作
            contexts.forEach(context -> {
                if (context.map(ContextChainStateListener.class::isInstance)) {
                    // 各个context都设置新状态
                    context.map(ContextChainStateListener.class::cast).onStateAcquired(newContextChainState);
                }
            });
        }
    }

    private void unMasterMe() {
        registryFilling.set(false);
        initialSubmitting.set(false);
        initialGathering.set(false);
        masterStateOnDevice.set(false);
        rpcRegistration.set(false);
    }
}
