package simfio.fs;

/** A FileSystem driven by tick(), which also ticks the files it opened. Call it from the loop thread. */
public interface TickableFileSystem extends FileSystem {
    void tick();
}
