// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.*;
import java.sql.Clob;
import java.sql.SQLException;

import de.simplicit.vjdbc.util.SQLExceptionHelper;

public class SerialClob implements Clob, Externalizable {
    private static final long serialVersionUID = 3904682695287452212L;
    
    private char[] _data;

    public SerialClob() {
    }

    public SerialClob(Clob other) throws SQLException {
        try {
            StringWriter sw = new StringWriter();
            Reader rd = other.getCharacterStream();
            char[] buff = new char[1024];
            int len;
            while((len = rd.read(buff)) > 0) {
                sw.write(buff, 0, len);
            }
            _data = sw.toString().toCharArray();
        } catch(IOException e) {
            throw new SQLException("Can't retrieve contents of Clob", e.toString());
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_data);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _data = (char[])in.readObject();
    }

    public long length() throws SQLException {
        return _data.length;
    }

    public String getSubString(long pos, int length) throws SQLException {
        return new String(_data, (int)pos - 1, length);
    }

    public Reader getCharacterStream() throws SQLException {
        return new StringReader(new String(_data));
    }

    public InputStream getAsciiStream() throws SQLException {
        try {
            return new ByteArrayInputStream(new String(_data).getBytes("US-ASCII"));
        } catch(UnsupportedEncodingException e) {
            throw SQLExceptionHelper.wrap(e);
        }
    }

    public long position(String searchstr, long start) throws SQLException {
        throw new UnsupportedOperationException("SerialClob.position");
    }

    public long position(Clob searchstr, long start) throws SQLException {
        throw new UnsupportedOperationException("SerialClob.position");
    }

    public int setString(long pos, String str) throws SQLException {
        throw new UnsupportedOperationException("SerialClob.setString");
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw new UnsupportedOperationException("SerialClob.setString");
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new UnsupportedOperationException("SerialClob.setAsciiStream");
    }

    public Writer setCharacterStream(long pos) throws SQLException {
        throw new UnsupportedOperationException("SerialClob.setCharacterStream");
    }

    public void truncate(long len) throws SQLException {
        throw new UnsupportedOperationException("SerialClob.truncate");
    }
}
