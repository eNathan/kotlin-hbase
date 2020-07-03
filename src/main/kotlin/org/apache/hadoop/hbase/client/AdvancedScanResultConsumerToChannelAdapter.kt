package org.apache.hadoop.hbase.client

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import java.lang.RuntimeException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

/**
 * Converts the HBase 2 async scan API to a Kotlin Channel.
 */
class AdvancedScanResultConsumerToChannelAdapter(
    val chan: SendChannel<Result> = Channel(),
    val ctx: CoroutineContext
): AdvancedScanResultConsumer {

    private var resumer: AdvancedScanResultConsumer.ScanResumer? = null
    private val clientClosedChannel = AtomicBoolean(false)

    // We can only cancel the underlying hbase scan inside of onNext() and onHeartbeat().
    // However, we can detect client cancellations in several ways: the scope (or the context in
    // in which the scope runs) got cancelled, or the client closed the receive channel. Both of
    // these are reasons to terminate the underlying scan because there is no way to proceed
    // sending elements. But we can't close it until onNext() or onHeartbeat() get called. So,
    // set set these indicators below (the AtomicBooleans) to indicate when these happen.
    // The next time we get a chance we'll close the scan.
    private val scope = CoroutineScope(ctx)
    private val clientCancelledContext = AtomicBoolean(false)
    private val closeScanWhenControllerBecomesAvailable
        get() = clientClosedChannel.get() || clientCancelledContext.get() || !scope.isActive

    init {
        chan.invokeOnClose {
            synchronized(this) {
                clientClosedChannel.set(true)
                resumer?.resume()
                scope.cancel()
            }
        }
    }

    override fun onComplete() {
        scope.cancel()
        chan.close()
    }

    override fun onError(error: Throwable?) {
        scope.cancel()
        chan.close(RuntimeException()) // TODO: Make descriptive exception
    }

    /**
     * Attempt to send all items in the iterator down the channel until
     * back pressure kicks in. Since we can't "peek" an iterator, we
     * actually have to pull an item off from it without knowing if we
     * can actaully send it down the channel. In this case, we return this
     * last item in the result.
     */
    private fun sendUntilBackpresureKicksIn(it: Iterator<Result>): Result? {
        var next: Result? = null
        while (it.hasNext()) {
            next = it.next()
            if (!chan.offer(next)) break
            next = null
        }
        return next
    }

    private fun sendRemainingItemsSuspending(
        unsentStraggler: Result?,
        it: Iterator<Result>) {
        scope.launch {
            try {
                unsentStraggler?.let { chan.send(it) }
                while (it.hasNext()) chan.send(it.next())
            } catch (e: CancellationException) {
                clientCancelledContext.set(true)
                throw e
            } catch (t: Throwable) {
                // TODO: warn
            } finally {
                resumer?.resume()
            }
        }

    }

    override fun onNext(results: Array<out Result>, controller: AdvancedScanResultConsumer.ScanController) = synchronized(this) {
        if (closeScanWhenControllerBecomesAvailable) {
            controller.terminate()
        } else {
            val it = results.iterator()
            val unsentStraggler = sendUntilBackpresureKicksIn(it)
            if (unsentStraggler != null || it.hasNext()) {
                resumer = controller.suspend()
                sendRemainingItemsSuspending(unsentStraggler, it)
            }
        }
    }

    override fun onHeartbeat(controller: AdvancedScanResultConsumer.ScanController) = synchronized(this) {
        if (closeScanWhenControllerBecomesAvailable) controller.terminate()
    }
}