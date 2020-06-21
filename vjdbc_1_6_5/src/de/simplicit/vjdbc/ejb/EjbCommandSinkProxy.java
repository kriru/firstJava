// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.ejb;

import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.command.CommandSink;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.util.SQLExceptionHelper;

import java.sql.SQLException;
import java.util.Properties;

public class EjbCommandSinkProxy implements CommandSink {
    private EjbCommandSink _ejbRef;

    public EjbCommandSinkProxy(EjbCommandSink ejbref) {
        _ejbRef = ejbref;
    }

    public UIDEx connect(String url, Properties props, Properties clientInfo, CallingContext ctx) throws SQLException {
        try {
            return _ejbRef.connect(url, props, clientInfo, ctx);
        } catch(Exception e) {
            throw SQLExceptionHelper.wrap(e);
        }
    }

    public Object process(Long connuid, Long uid, Command cmd, CallingContext ctx) throws SQLException {
        try {
            return _ejbRef.process(connuid, uid, cmd, ctx);
        } catch(Exception e) {
            throw SQLExceptionHelper.wrap(e);
        }
    }

    public void close() {
        _ejbRef = null;
    }
}
