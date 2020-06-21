// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.*;

public class StreamSerializer {
    public static byte[] toByteArray(InputStream is) throws IOException {
        if(is == null) {
            return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(is.available());
        byte[] buffer = new byte[1024];
        boolean loop = true;
        int offset = 0;

        while(loop) {
            int readBytes = is.read(buffer);

            if(readBytes >= 0) {
                baos.write(buffer, offset, readBytes);
                offset += readBytes;
            } else {
                loop = false;
            }
        }

        return baos.toByteArray();
    }

    public static InputStream toInputStream(byte[] buf) {
        if(buf == null) return null;
        return new ByteArrayInputStream(buf);
    }

    public static char[] toCharArray(Reader reader, int length) throws IOException {
        if(reader == null) {
            return null;
        }

        CharArrayWriter caw = new CharArrayWriter(length);
        char[] buffer = new char[1024];
        int offset = 0;

        while(length > 0) {
            int readChars = reader.read(buffer);

            if(readChars >= 0) {
                caw.write(buffer, offset, Math.min(readChars, length));
                length -= readChars;
            }
        }

        return caw.toCharArray();
    }

    public static Reader toReader(char[] buf) {
        if(buf == null) return null;
        return new CharArrayReader(buf);
    }
}