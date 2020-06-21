// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.server.ejb;

import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.server.command.CommandProcessor;
import de.simplicit.vjdbc.util.SQLExceptionHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ejb.CreateException;
import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class EjbCommandSinkBean implements SessionBean {
    private static final long serialVersionUID = 3257291331100423987L;

    private static Log _logger = LogFactory.getLog(EjbCommandSinkBean.class);

    private Context _ejbContext;
    private transient CommandProcessor _processor;

    public void setSessionContext(SessionContext sessionContext) throws EJBException {
        // _context = sessionContext;
    }

    public void ejbCreate() throws CreateException, EJBException {
        try {
            InitialContext ictx = new InitialContext();
            _ejbContext = (Context) ictx.lookup("java:comp/env");
        } catch (NamingException e) {
            throw new CreateException(e.toString());
        }

        _processor = CommandProcessor.getInstance();
    }

    public void ejbRemove() throws EJBException {
        _processor = null;
    }

    public void ejbActivate() throws EJBException {
    }

    public void ejbPassivate() throws EJBException {
    }

    public UIDEx connect(String url, Properties props, Properties clientInfo, CallingContext ctx) throws SQLException {
        try {
            // DataSource ermitteln
            _logger.info("Getting DataSource from environment context");
            DataSource dataSource = (DataSource) _ejbContext.lookup("jdbc/database");
            Connection conn = dataSource.getConnection();
            UIDEx reg = _processor.registerConnection(conn, null, clientInfo, ctx);
            _logger.info("Registered " + conn.getClass().getName() + " with UID " + reg);
            return reg;
        } catch (Exception e) {
            _logger.error(url, e);
            throw SQLExceptionHelper.wrap(e);
        }
    }

    public Object process(Long connuid, Long uid, Command cmd, CallingContext ctx) throws SQLException {
        return _processor.process(connuid, uid, cmd, ctx);
    }
}
