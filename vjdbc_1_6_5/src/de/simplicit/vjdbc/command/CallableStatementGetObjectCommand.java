// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import de.simplicit.vjdbc.serial.SerializableTransport;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class CallableStatementGetObjectCommand implements Command {
    static final long serialVersionUID = 7045834396073252820L;

    private int _index;
    private String _parameterName;
    private Map _map;

    public CallableStatementGetObjectCommand() {
    }

    public CallableStatementGetObjectCommand(int index) {
        _index = index;
        _map = null;
    }

    public CallableStatementGetObjectCommand(int index, Map map) {
        _index = index;
        _map = map;
    }

    public CallableStatementGetObjectCommand(String paramName) {
        _parameterName = paramName;
        _map = null;
    }

    public CallableStatementGetObjectCommand(String paramName, Map map) {
        _parameterName = paramName;
        _map = map;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeObject(_parameterName);
        out.writeObject(_map);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = (String)in.readObject();
        _map = (Map)in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        Object result;

        if(_parameterName != null) {
            if(_map != null) {
                result = cstmt.getObject(_parameterName, _map);
            } else {
                result = cstmt.getObject(_parameterName);
            }
        } else {
            if(_map != null) {
                result = cstmt.getObject(_index, _map);
            } else {
                result = cstmt.getObject(_index);
            }
        }
        
        // ResultSets are handled by the caller
        if(result instanceof ResultSet) {
            return result;
        }

        // Any other type must be Serializable to be transported
        if(result == null || result instanceof Serializable) {
            return new SerializableTransport(result, ctx.getCompressionMode(), ctx.getCompressionThreshold());
        } 

        throw new SQLException("Object of type " + result.getClass().getName() + " is not serializable");
    }

    public String toString() {
        return "CallableStatementGetObjectCommand";
    }
}
