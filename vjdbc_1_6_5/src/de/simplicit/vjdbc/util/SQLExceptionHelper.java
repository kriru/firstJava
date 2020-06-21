// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * SQLExceptionHelper wraps driver-specific exceptions in a generic SQLException.
 */
public class SQLExceptionHelper {
    public static SQLException wrap(Throwable t) {
        return wrapThrowable(t);
    }
    
    public static SQLException wrap(SQLException ex) {
        if(isSQLExceptionGeneric(ex)) {
            return ex;
        }
        else {
            return wrapSQLException(ex);
        }
    }
    
    private static boolean isSQLExceptionGeneric(SQLException ex) {
        boolean exceptionIsGeneric = true;
        
        // Check here if all chained SQLExceptions can be serialized, there may be
        // vendor specific SQLException classes which can't be delivered to the client
        SQLException loop = ex;
        while(loop != null && exceptionIsGeneric) {
            exceptionIsGeneric = loop.getClass().equals(SQLException.class) || loop.getClass().equals(SQLWarning.class);
            loop = loop.getNextException();
        }
        
        return exceptionIsGeneric;
    }

    private static SQLException wrapSQLException(SQLException ex) {
        SQLException ex2 = new SQLException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        if(ex.getNextException() != null) {
            ex2.setNextException(wrapSQLException(ex.getNextException()));
        }
        return ex2;
    }
    
    private static SQLException wrapThrowable(Throwable t) {
        // Then check if a cause is present
        if(JavaVersionInfo.use14Api && t.getCause() != null) {
            return wrapThrowable(t.getCause());
        }
        // Nothing to do, wrap the thing in a generic SQLException
        else {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            return new SQLException(sw.toString());
        }
    }
}
