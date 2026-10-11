package simfio.fs;

import java.util.Random;

class SimulatedFileSystemContractTest extends FileSystemContractTest {
    private final SimulatedFileSystem fileSystem = new SimulatedFileSystem(new Random(1), 1, 5);

    @Override
    TickableFileSystem fileSystem() {
        return fileSystem;
    }
}
