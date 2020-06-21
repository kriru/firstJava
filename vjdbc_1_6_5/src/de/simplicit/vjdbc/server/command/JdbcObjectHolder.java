// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.server.command;

import de.simplicit.vjdbc.serial.CallingContext;

class JdbcObjectHolder {
    private Object _jdbcObject;
    private CallingContext _callingContext;
    
    JdbcObjectHolder(Object jdbcObject, CallingContext ctx) {
        _jdbcObject = jdbcObject;
        _callingContext = ctx;
    }
    
    CallingContext getCallingContext() {
        return _callingContext;
    }

    Object getJdbcObject() {
        return _jdbcObject;
    }
}
