package com.kernelflux.vortexbus

import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.*
import java.lang.ref.WeakReference
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.Flow


data class EventTrace(
    val traceId: String,
    val key: String,
    val group: String,
    val action: String, // "post", "consume"
    val who: String,    // "sender", "receiver"
    val timestamp: Long,
    val thread: String,
    val stack: String,
    val cost: Long? = null // 仅消费时有
)

object VortexBus {

    // region 配置
    var maxHistory = 20
    var enableLog = true
    var monitor: ((EventTrace) -> Unit)? = null
    var debugMode = true // 可由外部设置
    // endregion

    // 普通事件（group+key -> 分发队列）
    private val eventChannels: ConcurrentHashMap<String, Channel<EventEnvelope>> =
        ConcurrentHashMap()

    // 普通事件订阅者（group+key -> 优先级队列）
    private val eventObservers: ConcurrentHashMap<String, PriorityBlockingQueue<ObserverWrapper>> =
        ConcurrentHashMap()

    // 粘性事件历史（group+key -> 事件历史队列）
    private val stickyHistoryMap: ConcurrentHashMap<String, LinkedList<VersionedEvent>> =
        ConcurrentHashMap()

    // 粘性事件订阅者Map（group+key -> 订阅者弱引用列表）
    private val stickyObserverMap: ConcurrentHashMap<String, MutableList<WeakReference<Job>>> =
        ConcurrentHashMap()

    // 事件分发traceId生成
    private val traceCounter = AtomicLong(0)

    // region 普通事件

    /**
     * 发送普通事件
     */
    @JvmStatic
    fun post(
        key: String,
        value: Any,
        group: String = "default",
        sender: String = Thread.currentThread().name
    ) {
        val traceId = "evt-${traceCounter.incrementAndGet()}"
        val fullKey = "$group#$key"
        logTrace(
            EventTrace(
                traceId, key, group, "post", "sender",
                System.currentTimeMillis(), sender, Throwable().stackTraceToString()
            )
        )
        val envelope = EventEnvelope(traceId, value)
        val channel = eventChannels.getOrPut(fullKey) { Channel(Channel.UNLIMITED) }
        CoroutineScope(Dispatchers.Default).launch {
            channel.send(envelope)
        }
        // 分发给优先级队列订阅者（异步，避免递归/死锁）
        eventObservers[fullKey]?.let { queue ->
            CoroutineScope(Dispatchers.Default).launch {
                val toRemove = mutableListOf<ObserverWrapper>()
                for (wrapper in queue) {
                    if (wrapper.jobRef.get()?.isActive != true) {
                        toRemove.add(wrapper)
                        continue
                    }
                    val intercepted = tryIntercept(wrapper, value)
                    if (!intercepted) {
                        launch(wrapper.dispatcher) {
                            val start = System.currentTimeMillis()
                            try {
                                wrapper.observer(value)
                            } catch (e: ClassCastException) {
                                handleTypeError(key, value, e)
                            }
                            logTrace(
                                EventTrace(
                                    traceId,
                                    key,
                                    group,
                                    "consume",
                                    "receiver",
                                    System.currentTimeMillis(),
                                    Thread.currentThread().name,
                                    Throwable().stackTraceToString(),
                                    cost = System.currentTimeMillis() - start
                                )
                            )
                        }
                    }
                    if (intercepted) break // 拦截后不再向下分发
                }
                queue.removeAll(toRemove)
            }
        }
    }

    /**
     * 订阅普通事件（优先级、拦截、批量订阅、分组、自动解绑）
     */
    @JvmStatic
    fun observe(
        keys: List<String>,
        owner: LifecycleOwner,
        group: String = "default",
        priority: Int = 0,
        dispatcher: CoroutineDispatcher = Dispatchers.Main,
        intercept: ((Any) -> Boolean)? = null,
        observer: (key: String, value: Any) -> Unit
    ): List<Job> {
        return keys.map { key ->
            observeSingle(key, owner, group, priority, dispatcher, intercept) { value ->
                observer(key, value)
            }
        }
    }

    @JvmStatic
    private fun observeSingle(
        key: String,
        owner: LifecycleOwner,
        group: String,
        priority: Int,
        dispatcher: CoroutineDispatcher,
        intercept: ((Any) -> Boolean)?,
        observer: (Any) -> Unit
    ): Job {
        val fullKey = "$group#$key"
        val job = owner.lifecycleScope.launch(dispatcher) {
            // nothing, 只靠 post 主动分发
        }
        val wrapper = ObserverWrapper(
            priority = priority,
            intercept = intercept,
            observer = observer,
            jobRef = WeakReference(job),
            dispatcher = dispatcher
        )

        val queue = eventObservers.getOrPut(fullKey) {
            PriorityBlockingQueue<ObserverWrapper>(11, compareByDescending { it.priority })
        }

        queue.add(wrapper)
        logTrace(
            EventTrace(
                "obs-${traceCounter.incrementAndGet()}",
                key,
                group,
                "observe",
                "receiver",
                System.currentTimeMillis(),
                Thread.currentThread().name,
                Throwable().stackTraceToString()
            )
        )
        owner.lifecycle.addObserver(LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_DESTROY) {
                job.cancel()
                queue.remove(wrapper)
                logTrace(
                    EventTrace(
                        "unbind-${traceCounter.incrementAndGet()}",
                        key,
                        group,
                        "unbind",
                        "receiver",
                        System.currentTimeMillis(),
                        Thread.currentThread().name,
                        Throwable().stackTraceToString()
                    )
                )
            }
        })
        return job
    }

    /**
     * 冷流接口，支持Replay
     */
    @JvmStatic
    fun asFlow(
        key: String,
        group: String = "default",
        replay: Int = 0 // 0为不replay
    ): Flow<Any> {
        val fullKey = "$group#$key"
        val channel = eventChannels.getOrPut(fullKey) { Channel(Channel.UNLIMITED) }
        val replayBuffer = LinkedList<EventEnvelope>()
        return channel.consumeAsFlow()
            .onEach { envelope ->
                if (replay > 0) {
                    replayBuffer.addLast(envelope)
                    if (replayBuffer.size > replay) replayBuffer.removeFirst()
                }
            }
            .map { envelope ->
                logTrace(
                    EventTrace(
                        envelope.traceId,
                        key,
                        group,
                        "consume",
                        "receiver",
                        System.currentTimeMillis(),
                        Thread.currentThread().name,
                        Throwable().stackTraceToString()
                    )
                )
                envelope.value
            }
            .let { flow ->
                if (replay > 0) flow.onStart { replayBuffer.forEach { emit(it.value) } }
                else flow
            }
    }

    // endregion

    // region 粘性事件

    /**
     * 发送粘性事件
     */
    @JvmStatic
    fun postSticky(
        key: String,
        value: Any,
        group: String = "default",
        sender: String = Thread.currentThread().name
    ) {
        val traceId = "evt-${traceCounter.incrementAndGet()}"
        val fullKey = "$group#$key"
        logTrace(
            EventTrace(
                traceId, key, group, "postSticky", "sender",
                System.currentTimeMillis(), sender, Throwable().stackTraceToString()
            )
        )
        val history = stickyHistoryMap.getOrPut(fullKey) { LinkedList() }
        val version = (history.lastOrNull()?.version ?: 0) + 1
        history.add(VersionedEvent(version, value, traceId))
        if (history.size > maxHistory) history.removeFirst()
        notifyStickyObservers(fullKey)
    }

    /**
     * 批量订阅粘性事件（优先级、拦截、分组、自动解绑）
     */
    @JvmStatic
    fun observeSticky(
        keys: List<String>,
        owner: LifecycleOwner,
        group: String = "default",
        minVersion: Int = 0,
        priority: Int = 0,
        dispatcher: CoroutineDispatcher = Dispatchers.Main,
        intercept: ((Any) -> Boolean)? = null,
        observer: (key: String, value: Any, version: Int) -> Unit
    ): List<Job> {
        return keys.map { key ->
            observeStickySingle(
                key,
                owner,
                group,
                minVersion,
                priority,
                dispatcher,
                intercept
            ) { value, version ->
                observer(key, value, version)
            }
        }
    }

    @JvmStatic
    private fun observeStickySingle(
        key: String,
        owner: LifecycleOwner,
        group: String,
        minVersion: Int,
        priority: Int,
        dispatcher: CoroutineDispatcher,
        intercept: ((Any) -> Boolean)?,
        observer: (Any, version: Int) -> Unit
    ): Job {
        val fullKey = "$group#$key"
        val job = owner.lifecycleScope.launch(dispatcher) {
            val history = stickyHistoryMap.getOrPut(fullKey) { LinkedList() }
            // 先补发历史
            history.filter { it.version > minVersion }.forEach {
                val intercepted = intercept?.invoke(it.value) == true
                if (!intercepted) observer(it.value, it.version)
                if (intercepted) return@forEach
            }
            // 持续监听新事件
            var lastVersion = history.lastOrNull()?.version ?: minVersion
            while (isActive) {
                delay(Long.MAX_VALUE) // 唤醒靠 postSticky
                val newHistory = stickyHistoryMap[fullKey] ?: break
                newHistory.filter { it.version > lastVersion }.forEach {
                    val intercepted = intercept?.invoke(it.value) == true
                    if (!intercepted) observer(it.value, it.version)
                    if (intercepted) return@forEach
                    lastVersion = it.version
                }
            }
        }
        val ref = WeakReference(job)
        stickyObserverMap.getOrPut(fullKey) { mutableListOf() }.add(ref)
        logTrace(
            EventTrace(
                "obsSticky-${traceCounter.incrementAndGet()}",
                key,
                group,
                "observeSticky",
                "receiver",
                System.currentTimeMillis(),
                Thread.currentThread().name,
                Throwable().stackTraceToString()
            )
        )
        owner.lifecycle.addObserver(LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_DESTROY) {
                job.cancel()
                stickyObserverMap[fullKey]?.remove(ref)
                logTrace(
                    EventTrace(
                        "unbindSticky-${traceCounter.incrementAndGet()}",
                        key,
                        group,
                        "unbindSticky",
                        "receiver",
                        System.currentTimeMillis(),
                        Thread.currentThread().name,
                        Throwable().stackTraceToString()
                    )
                )
            }
        })
        return job
    }

    /**
     * 粘性事件冷流接口，支持Replay
     */
    @JvmStatic
    fun asStickyFlow(
        key: String,
        group: String = "default",
        minVersion: Int = 0,
        replay: Int = 0 // 0为不replay
    ): Flow<Pair<Any, Int>> = channelFlow {
        val fullKey = "$group#$key"
        val history = stickyHistoryMap.getOrPut(fullKey) { LinkedList() }
        val replayList = history.filter { it.version > minVersion }
        val replayBuffer = LinkedList<VersionedEvent>()
        replayList.takeLast(replay).forEach {
            trySend(it.value to it.version)
            replayBuffer.add(it)
        }
        val job = observeStickyInternal(fullKey, minVersion) { value, version, traceId ->
            logTrace(
                EventTrace(
                    traceId,
                    key,
                    group,
                    "consumeSticky",
                    "receiver",
                    System.currentTimeMillis(),
                    Thread.currentThread().name,
                    Throwable().stackTraceToString()
                )
            )
            trySend(value to version)
            if (replay > 0) {
                replayBuffer.add(VersionedEvent(version, value, traceId))
                if (replayBuffer.size > replay) replayBuffer.removeFirst()
            }
        }
        awaitClose { job.cancel() }
    }

    @JvmStatic
    private fun observeStickyInternal(
        fullKey: String,
        minVersion: Int,
        observer: (Any, version: Int, traceId: String) -> Unit
    ): Job {
        val job = CoroutineScope(Dispatchers.Default).launch {
            var lastVersion = minVersion
            while (isActive) {
                delay(Long.MAX_VALUE) // 唤醒靠 postSticky
                val history = stickyHistoryMap[fullKey] ?: break
                history.filter { it.version > lastVersion }.forEach {
                    observer(it.value, it.version, it.traceId)
                    lastVersion = it.version
                }
            }
        }
        val ref = WeakReference(job)
        stickyObserverMap.getOrPut(fullKey) { mutableListOf() }.add(ref)
        return job
    }

    @JvmStatic
    private fun notifyStickyObservers(fullKey: String) {
        stickyObserverMap[fullKey]?.forEach { ref ->
            ref.get()?.let { job ->
                if (job.isActive) job.cancel("new event") // 触发重启
            }
        }
    }

    // endregion

    // region 事件追踪与日志

    @JvmStatic
    private fun logTrace(trace: EventTrace) {
        if (enableLog) println("[VortexBus] $trace")
        monitor?.invoke(trace)
    }

    // endregion

    // region 清理
    @JvmStatic
    fun clear(key: String, group: String = "default") {
        val fullKey = "$group#$key"
        eventChannels.remove(fullKey)
        eventObservers.remove(fullKey)
        stickyHistoryMap.remove(fullKey)
        stickyObserverMap.remove(fullKey)
    }

    @JvmStatic
    fun clearAll() {
        eventChannels.clear()
        eventObservers.clear()
        stickyHistoryMap.clear()
        stickyObserverMap.clear()
    }

    // endregion

    // 数据结构
    data class VersionedEvent(val version: Int, val value: Any, val traceId: String)
    data class EventEnvelope(val traceId: String, val value: Any)
    data class ObserverWrapper(
        val priority: Int,
        val intercept: ((Any) -> Boolean)?,
        val observer: (Any) -> Unit,
        val jobRef: WeakReference<Job>,
        val dispatcher: CoroutineDispatcher
    ) : Comparable<ObserverWrapper> {
        override fun compareTo(other: ObserverWrapper): Int = other.priority.compareTo(priority)
    }

    private fun tryIntercept(wrapper: ObserverWrapper, value: Any): Boolean {
        return try {
            wrapper.intercept?.invoke(value) == true
        } catch (e: ClassCastException) {
            handleTypeError("intercept", value, e)
            false
        }
    }

    private fun handleTypeError(key: String, value: Any, e: ClassCastException) {
        if (debugMode) throw e
        else println("[VortexBus][TypeError] key=$key, value=$value, error=${e.message}")
    }
}