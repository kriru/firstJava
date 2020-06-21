// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.Map;

public class SerialArray implements Array, Externalizable {
    private static final long serialVersionUID = 3256722892212418873L;
    
    private int _baseType;
    private String _baseTypeName;
    private Object _array;

    public SerialArray() {
    }

    public SerialArray(Array arr) throws SQLException {
        _baseType = arr.getBaseType();
        _baseTypeName = arr.getBaseTypeName();
        _array = arr.getArray();

        if(_baseType == Types.STRUCT) {
            Object[] orig = (Object[])_array;
            Struct[] cpy = new SerialStruct[orig.length];
            for(int i = 0; i < orig.length; i++) {
                cpy[i] = new SerialStruct((Struct)orig[i]);
            }
            _array = cpy;
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_baseType);
        out.writeObject(_baseTypeName);
        out.writeObject(_array);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _baseType = in.readInt();
        _baseTypeName = (String)in.readObject();
        _array = in.readObject();
    }

    public String getBaseTypeName() throws SQLException {
        return _baseTypeName;
    }

    public int getBaseType() throws SQLException {
        return _baseType;
    }

    public Object getArray() throws SQLException {
        return _array;
    }

    public Object getArray(Map map) throws SQLException {
        throw new UnsupportedOperationException("getArray(Map) not supported");
    }

    public Object getArray(long index, int count) throws SQLException {
        throw new UnsupportedOperationException("getArray(index, count) not supported");
    }

    public Object getArray(long index, int count, Map map) throws SQLException {
        throw new UnsupportedOperationException("getArray(index, count, Map) not supported");
    }

    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("getResultSet() not supported");
    }

    public ResultSet getResultSet(Map map) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(Map) not supported");
    }

    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(index, count) not supported");
    }

    public ResultSet getResultSet(long index, int count, Map map) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(index, count, Map) not supported");
    }
}
