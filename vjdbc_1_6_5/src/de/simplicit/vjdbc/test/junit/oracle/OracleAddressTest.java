// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.test.junit.oracle;

import de.simplicit.vjdbc.test.junit.VJdbcTest;
import de.simplicit.vjdbc.test.junit.general.AddressTest;
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleAddressTest extends AddressTest {
    public static Test suite() throws Exception {
        TestSuite suite = new TestSuite();
        
        VJdbcTest.addAllTestMethods(suite, OracleAddressTest.class);

        TestSetup wrapper = new TestSetup(suite) {
            protected void setUp() throws Exception {
                new OracleAddressTest("").oneTimeSetup();
            }

            protected void tearDown() throws Exception {
                new OracleAddressTest("").oneTimeTearDown();
            }
        };

        return wrapper;
    }

    public OracleAddressTest(String s) {
        super(s);
    }

    protected Connection createNativeDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:oracle:thin:@mikepc:1521:orcl", "system", "system");
    }
    
	protected String getVJdbcPassword() {
		return "vjdbc";
	}

	protected String getVJdbcUser() {
		return "vjdbc";
	}

    protected String getVJdbcDatabaseShortcut() {
        return "OracleDB";
    }
}
