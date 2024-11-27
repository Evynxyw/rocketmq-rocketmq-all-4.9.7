/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    /**
     * 文件的引用计数，初始化为 1，表示有一个线程正在引用这个文件。
     * 当一个新的线程也对其引用时，需要 +1，不引用了则 -1。
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    /** 文件的可用状态 */
    protected volatile boolean available = true;

    /** 文件的清理状态，为 true 则说明文件已关闭，并且 refCount 为 0 了，可以删除此文件了 */
    protected volatile boolean cleanupOver = false;

    /** 第一次 shutdown 的时间戳 */
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        // 检查当前文件是否可用
        if (this.isAvailable()) {
            // 先获取 refCount 的值，其默认为 1，先 get 出来，然后 +1，判断 get 出来的值是否大于 0，大于则说明没人占用此文件，
            // 然后 + 1，再返回 true，表示持有了这个文件。
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                // 如果获取 refCount 的值小于等于 0，则说明这次的累加操作要回滚
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            // 将 available 置为 false
            this.available = false;
            // 更新关闭时间
            this.firstShutdownTimestamp = System.currentTimeMillis();
            // 释放对象的引用计数
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放引用
     */
    public void release() {
        // 先减 1，然后获取到减去 1 的结果
        long value = this.refCount.decrementAndGet();
        // 如果大于 0，则说明还有其他地方在引用，不能释放
        if (value > 0)
            return;

        // 加锁，执行 cleanup 清除操作
        synchronized (this) {
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
