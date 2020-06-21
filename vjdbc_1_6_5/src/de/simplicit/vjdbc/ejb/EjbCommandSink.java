// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.ejb;

import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.UIDEx;

import javax.ejb.EJBException;
import javax.ejb.EJBObject;
import java.util.Properties;

public interface EjbCommandSink extends EJBObject {
    UIDEx connect(String url, Properties props, Properties clientInfo, CallingContext ctx) throws EJBException;

    Object process(Long connuid, Long uid, Command cmd, CallingContext ctx) throws EJBException;
}
