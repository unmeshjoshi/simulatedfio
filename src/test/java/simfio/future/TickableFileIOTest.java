package simfio.future;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class TickableFileIOTest {

    @TempDir
    Path tempDir;
    @Test
    void write() throws IOException {
        Path filePath = tempDir.resolve("test.txt");
        FileChannel channel = FileChannel.open(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        TickableFileIO fileIO = new TickableFileIO(channel);
        TickCompletableFuture<Integer> writeFuture = fileIO.write(ByteBuffer.wrap("test string".getBytes()), 0);
        while (writeFuture.isPending()) {
            fileIO.tick();
        }

        File file = filePath.toFile();
        FileReader reader = new FileReader(file);
        assertEquals("test string", reader.readAllAsString());
    }
}