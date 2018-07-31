/*
 * Copyright (c) 2015 Pantheon Technologies s.r.o. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.openflowjava.protocol.impl.core.connection;

import com.google.common.base.Preconditions;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.opendaylight.openflowjava.protocol.api.connection.OutboundQueueHandler;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.BarrierInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.openflow.protocol.rev130731.OfHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OutboundQueueManager<T extends OutboundQueueHandler> extends
        AbstractOutboundQueueManager<T, StackedOutboundQueue> {
    private static final Logger LOG = LoggerFactory.getLogger(OutboundQueueManager.class);

    private final int maxNonBarrierMessages;
    private final long maxBarrierNanos;

    // Updated from netty only
    private boolean barrierTimerEnabled;
    private long lastBarrierNanos = System.nanoTime();
    private int nonBarrierMessages;

    // Passed to executor to request a periodic barrier check
    private final Runnable barrierRunnable = new Runnable() {
        @Override
        public void run() {
            barrier();
        }
    };

    /*
        在ConnectionAdapterImpl中, T handler传入的是OutboundQueueProviderImpl
     */
    OutboundQueueManager(final ConnectionAdapterImpl parent, final InetSocketAddress address, final T handler,
        final int maxNonBarrierMessages, final long maxBarrierNanos) {
        super(parent, address, handler);
        Preconditions.checkArgument(maxNonBarrierMessages > 0);
        this.maxNonBarrierMessages = maxNonBarrierMessages;
        Preconditions.checkArgument(maxBarrierNanos > 0);
        this.maxBarrierNanos = maxBarrierNanos;
    }

    /*
        多态, `new StackedOutboundQueue(this)`是currentQueue
     */
    @Override
    protected StackedOutboundQueue initializeStackedOutboudnqueue() {
        return new StackedOutboundQueue(this);
    }

    /*
        根据barrier时间间隔(默认500 nano seconds)调度eventLoop发送消息
     */
    private void scheduleBarrierTimer(final long now) {
        // 上一次发送barrier时间 + barrier时间间隔(默认500) 为下一次计划时间
        long next = lastBarrierNanos + maxBarrierNanos;
        if (next < now) {
            // 如果已经过程了下一次时间（now已经过了），从当前时间 + barrier间隔为next发送barrier时间
            LOG.trace("Attempted to schedule barrier in the past, reset maximum)");
            next = now + maxBarrierNanos;
        }

        final long delay = next - now;
        LOG.trace("Scheduling barrier timer {}us from now", TimeUnit.NANOSECONDS.toMicros(delay));
        // next-now: 在这个时间后执行barrierRunnable
        parent.getChannel().eventLoop().schedule(barrierRunnable, next - now, TimeUnit.NANOSECONDS);
        barrierTimerEnabled = true;
    }

    private void scheduleBarrierMessage() {
        final Long xid = currentQueue.reserveBarrierIfNeeded();
        if (xid == null) {
            LOG.trace("Queue {} already contains a barrier, not scheduling one", currentQueue);
            return;
        }

        currentQueue.commitEntry(xid, getHandler().createBarrierRequest(xid), null);
        LOG.trace("Barrier XID {} scheduled", xid);
}


    /**
     * Periodic barrier check.
     */
    protected void barrier() {
        LOG.debug("Channel {} barrier timer expired", parent.getChannel());
        barrierTimerEnabled = false;
        if (shuttingDown) {
            LOG.trace("Channel shut down, not processing barrier");
            return;
        }

        if (currentQueue.isBarrierNeeded()) {
            LOG.trace("Sending a barrier message");
            scheduleBarrierMessage();
        } else {
            LOG.trace("Barrier not needed, not issuing one");
        }
    }

    /**
     * Write a message into the underlying channel.
     *
     * @param now Time reference for 'now'. We take this as an argument, as
     *            we need a timestamp to mark barrier messages we see swinging
     *            by. That timestamp does not need to be completely accurate,
     *            hence we use the flush start time. Alternative would be to
     *            measure System.nanoTime() for each barrier -- needlessly
     *            adding overhead.
     */
    @Override
    void writeMessage(final OfHeader message, final long now) {
        super.writeMessage(message, now);
        if (message instanceof BarrierInput) {
            LOG.trace("Barrier message seen, resetting counters");
            nonBarrierMessages = 0;
            lastBarrierNanos = now;
        } else {
            nonBarrierMessages++;
            // 判断累计发送了多少个消息，当梳理大于maxNonBarrierMessages强制发送一次
            if (nonBarrierMessages >= maxNonBarrierMessages) {
                LOG.trace("Scheduled barrier request after {} non-barrier messages", nonBarrierMessages);
                scheduleBarrierMessage();
            } else if (!barrierTimerEnabled) {
                // 次数没累计足够, 判断barrier消息时间间隔, 发送barrier消息
                scheduleBarrierTimer(now);
            }
        }
    }
}
