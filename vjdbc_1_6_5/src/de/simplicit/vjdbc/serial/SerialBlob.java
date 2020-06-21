// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;

public class SerialBlob implements Blob, Externalizable {
    private static final long serialVersionUID = 3258134639489857079L;
    
    private byte[] _data;

    public SerialBlob() {
    }

    public SerialBlob(Blob other) throws SQLException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = other.getBinaryStream();
            byte[] buff = new byte[1024];
            int len;
            while((len = is.read(buff)) > 0) {
                baos.write(buff, 0, len);
            }
            _data = baos.toByteArray();
        } catch(IOException e) {
            throw new SQLException("Can't retrieve contents of Blob", e.toString());
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_data);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _data = (byte[])in.readObject();
    }

    public long length() throws SQLException {
        return _data.length;
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        byte[] result = new byte[length];
        System.arraycopy(_data, (int)pos - 1, result, 0, length);
        return result;
    }

    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(_data);
    }

    public long position(byte pattern[], long start) throws SQLException {
        throw new UnsupportedOperationException("Blob.position");
    }

    public long position(Blob pattern, long start) throws SQLException {
        throw new UnsupportedOperationException("Blob.position");
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new UnsupportedOperationException("Blob.setBytes");
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw new UnsupportedOperationException("Blob.setBytes");
    }

    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new UnsupportedOperationException("Blob.setBinaryStream");
    }

    public void truncate(long len) throws SQLException {
        throw new UnsupportedOperationException("Blob.truncate");
    }
}
