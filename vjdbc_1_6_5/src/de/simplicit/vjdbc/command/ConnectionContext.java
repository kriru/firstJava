// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.sql.SQLException;

/**
 * This interface provides access to connection specific context for all commands
 * executed on the server.
 */
public interface ConnectionContext {
    // Accessor methods to all registered JDBC objects
    Object getJDBCObject(Object key);
    void addJDBCObject(Object key, Object partner);
    Object removeJDBCObject(Object key);
    // Compression
    int getCompressionMode();
    long getCompressionThreshold();
    // Row-Packets
    int getRowPacketSize();
    String getCharset();
    // Resolve and check query
    String resolveOrCheckQuery(String sql) throws SQLException;
}
