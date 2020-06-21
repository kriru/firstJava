// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import de.simplicit.vjdbc.serial.UIDEx;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.sql.SQLException;

public class DestroyCommand implements Command {
    static final long serialVersionUID = 4457392123395584636L;

    private static Log _logger = LogFactory.getLog(DestroyCommand.class);
    private Long _uid;
    private int _interfaceType;

    public DestroyCommand() {
    }

    public DestroyCommand(UIDEx regentry, int interfaceType) {
        _uid = regentry.getUID();
        _interfaceType = interfaceType;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_uid.longValue());
        out.writeInt(_interfaceType);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _uid = new Long(in.readLong());
        _interfaceType = in.readInt();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        Object removed = ctx.removeJDBCObject(_uid);

        // Check for identity
        if(removed == target) {
            if(_logger.isDebugEnabled()) {
                _logger.debug("Removed " + target.getClass().getName() + " with UID " + _uid);
            }
            try {
                Class targetClass = JdbcInterfaceType._interfaces[_interfaceType];
                Method mth = targetClass.getDeclaredMethod("close", new Class[0]);
                mth.invoke(target, (Object[])null);
                if(_logger.isDebugEnabled()) {
                    _logger.debug("Invoked close() successfully");
                }
            } catch(NoSuchMethodException e) {
                // Object doesn't support close()
            } catch(Exception e) {
                if(_logger.isDebugEnabled()) {
                    _logger.debug("Invocation of close() failed", e);
                }
            }
        } else {
            if(_logger.isWarnEnabled()) {
                _logger.warn("Target object " + target + " wasn't registered with UID " + _uid);
            }
        }
        return null;
    }

    public String toString() {
        return "DestroyCommand";
    }
}
