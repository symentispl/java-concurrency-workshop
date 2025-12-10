package pl.symentis.concurrent.doublebuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DoubleBufferedDiskWriter {

    private final IORegion ioRegionA;
    private final IORegion ioRegionB;
    private volatile IORegion currentIoRegion;

    private final Lock lock = new ReentrantLock();
    private volatile boolean closed = false;

    private final ExecutorService ioExecutor;
    private final FileChannel fileChannel;

    public DoubleBufferedDiskWriter(Path filePath, int bufferSize) throws IOException {
        this.fileChannel = FileChannel.open(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        this.ioRegionA = new IORegion(ByteBuffer.allocateDirect(bufferSize), lock.newCondition(), lock.newCondition());
        this.ioRegionB = new IORegion(ByteBuffer.allocateDirect(bufferSize), lock.newCondition(), lock.newCondition());
        this.currentIoRegion = ioRegionA;

        this.ioExecutor = Executors.newSingleThreadExecutor(r -> {
            var thread = new Thread(r);
            thread.setName("double-buffered-writer-io-thread");
            return thread;
        });

        ioExecutor.submit(this::ioLoop);
    }

    public void write(ByteBuffer data) throws InterruptedException, IOException {
        while (true) {
            if (closed) throw new IOException("Writer closed");
            lock.lock();
            try {
                var ioRegion = currentIoRegion;
                if (ioRegion.buffer.remaining() >= data.remaining()) {
                    // data fits in current buffer
                    ioRegion.buffer.put(data);
                    if (ioRegion.buffer.remaining() == 0) {
                        // Buffer is now isFull - fast track to signal IO thread and wait for flush
                        ioRegion.ioInProgress = true;
                        ioRegion.isFull.signal();
                        // Wait for this buffer to be isFlushed
                        while (ioRegion.ioInProgress) {
                            ioRegion.isFlushed.await();
                        }
                    }
                    // If buffer not isFull, return immediately without waiting
                    return;
                } else {
                    // data does not fit in current buffer - signal flush and retry
                    ioRegion.ioInProgress = true;
                    ioRegion.isFull.signal();
                    // retry, we have to free the lock, so IO thread can proceed
                }
                // TODO data is bigger then buffer (out-of-band-execution)
            } finally {
                lock.unlock();

            }
        }
    }

    private void ioLoop() {
        while (true) {
            lock.lock();
            try {
                // Check closed flag while holding the lock
                if (closed) {
                    var ioRegion = currentIoRegion;
                    if (ioRegion.buffer.position() == 0) {
                        // If closed and buffer is empty, exit
                        break;
                    }
                }

                var ioRegion = currentIoRegion;
                ioRegion.isFull.await();

                // Check again after waking up
                if (closed && ioRegion.buffer.position() == 0) {
                    // If closed and buffer is empty, exit
                    break;
                }

                // Prepare buffer for reading by flipping it
                ioRegion.buffer.flip();
                fileChannel.write(ioRegion.buffer);
                ioRegion.buffer.clear();
                // Mark IO as done and signal
                ioRegion.ioInProgress = false;
                // Switch to the other buffer
                currentIoRegion = ioRegion == ioRegionA ? ioRegionB : ioRegionA;
                ioRegion.isFlushed.signalAll();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    public void close() throws InterruptedException, IOException {
        lock.lock();
        try {
            closed = true;
            // TODO Hacky signal both buffers to wake up IO thread
            if (ioRegionA.buffer.position() > 0) {
                ioRegionA.ioInProgress = true;
            }
            if (ioRegionB.buffer.position() > 0) {
                ioRegionB.ioInProgress = true;
            }
            // Always signal both to wake up IO thread so it can check `closed` flag
            ioRegionA.isFull.signalAll();
            ioRegionB.isFull.signalAll();
        } finally {
            lock.unlock();
        }

        // Shutdown the executor and wait for pending writes
        ioExecutor.shutdown();
        ioExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);

        // Close the file channel
        fileChannel.close();
    }

     static final class IORegion {
        private final ByteBuffer buffer;
        private final Condition isFull;
        private final Condition isFlushed;
        private volatile boolean ioInProgress = false;

        IORegion(ByteBuffer buffer, Condition isFull, Condition isFlushed) {
            this.buffer = buffer;
            this.isFull = isFull;
            this.isFlushed = isFlushed;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof IORegion that)) return false;
            return Objects.equals(buffer, that.buffer) && Objects.equals(isFull, that.isFull) && Objects.equals(isFlushed, that.isFlushed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(buffer, isFull, isFlushed);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("DoubleBuffer{");
            sb.append("buffer=").append(buffer);
            sb.append(", isFull=").append(isFull);
            sb.append(", isFlushed=").append(isFlushed);
            sb.append(", ioInProgress=").append(ioInProgress);
            sb.append('}');
            return sb.toString();
        }
    }
}
