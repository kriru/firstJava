// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

public class SerialRef implements Ref {
    private String _baseTypeName;
    private Object _javaObject;

    public SerialRef(Ref ref) throws SQLException {
        _baseTypeName = ref.getBaseTypeName();
        _javaObject = ref.getObject();
    }

    public String getBaseTypeName() throws SQLException {
        return _baseTypeName;
    }

    public Object getObject(Map map) throws SQLException {
        throw new UnsupportedOperationException("Ref.getObject(Map) not supported");
    }

    public Object getObject() throws SQLException {
        return _javaObject;
    }

    public void setObject(Object value) throws SQLException {
        throw new UnsupportedOperationException("Ref.setObject(Object) not supported");
    }
}
