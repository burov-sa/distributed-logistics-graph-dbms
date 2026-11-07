package ru.itmo.graphdb.network.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Утилита сериализации/десериализации {@link Frame} поверх
 * {@link DataInputStream}/{@link DataOutputStream}.
 *
 * <p>Формат кадра на проводе:
 * <pre>
 *   [1 байт: FrameType.code] [2+N байт: writeUTF(payload)]
 * </pre>
 *
 * <p>Класс утилитарный: экземпляры не создаются.
 */
public final class Frames {

    private Frames() {
        // no instances
    }

    /**
     * Записывает кадр в поток, но <b>не</b> вызывает {@code flush()}.
     * Flush оставлен на вызывающего — это позволяет писать пачку кадров
     * и сбрасывать их одним syscall.
     */
    public static void write(DataOutputStream out, Frame frame) throws IOException {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(frame, "frame");
        out.writeByte(frame.type().code());
        out.writeUTF(frame.payload());
    }

    /** Записывает кадр и сразу сбрасывает буфер. Удобно для одиночных ответов/push. */
    public static void writeAndFlush(DataOutputStream out, Frame frame) throws IOException {
        write(out, frame);
        out.flush();
    }

    /**
     * Читает один кадр из потока. Блокирующий вызов.
     *
     * @throws IOException         при сетевой ошибке / закрытии сокета
     * @throws java.io.EOFException при нормальном закрытии соединения
     *                             (пробрасывается как есть — вызывающий
     *                             отличает его от прочих I/O-ошибок)
     */
    public static Frame read(DataInputStream in) throws IOException {
        Objects.requireNonNull(in, "in");
        byte code = in.readByte();
        FrameType type = FrameType.fromCode(code);
        String payload = in.readUTF();
        return new Frame(type, payload);
    }
}