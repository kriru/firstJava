// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.test.junit.oracle;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class BlobClobTest extends Oracle9iTest {
    public static Test suite() {
        TestSuite suite = new TestSuite();

        suite.addTest(new BlobClobTest("testInsertBlobs"));
        suite.addTest(new BlobClobTest("testInsertClobs"));

        TestSetup wrapper = new TestSetup(suite) {
            protected void setUp() throws Exception {
                new BlobClobTest("").oneTimeSetup();
            }

            protected void tearDown() throws Exception {
                new BlobClobTest("").oneTimeTearDown();
            }
        };

        return wrapper;
    }

    public BlobClobTest(String s) {
        super(s);
    }

    protected void oneTimeSetup() throws Exception {
        super.oneTimeSetup();

        Connection connVJdbc = createVJdbcConnection();

        System.out.println("Creating tables ...");
        Statement stmt = connVJdbc.createStatement();
        dropTables(stmt, new String[]{"blobs", "clobs"});
        stmt.executeUpdate("create table blobs (id int, someblob blob)");
        stmt.executeUpdate("create table clobs (id int, someclob clob)");
        stmt.close();
        connVJdbc.close();
    }

    public void testInsertBlobs() throws Exception {
        byte[] blobdata = new byte[] {
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77
        };
        PreparedStatement pstmt = _connVJdbc.prepareStatement("insert into blobs values(?, ?)");
        for(int i = 1; i <= 10; i++) {
            pstmt.setInt(1, i);
            pstmt.setBinaryStream(2, new ByteArrayInputStream(blobdata), blobdata.length);
            pstmt.addBatch();
        }
        assertEquals(pstmt.executeBatch().length, 10);
    }

    public void testInsertClobs() throws Exception {
        StringBuffer sb = new StringBuffer();
        /*
         * The upper bound of the loop was originally 1000 which resulted in a clob of about
         * 8000 bytes. The Thin-Driver doesn't seem to cope with that, it's just possible
         * to write up to 4k (see http://helma.org/pipermail/helma-user/2004-August/002723.html)
         */
        for(int i = 0; i < 100; i++) {
            sb.append("CLOBDATA");
        }
        String clobdata = sb.toString();
        PreparedStatement pstmt = _connVJdbc.prepareStatement("insert into clobs values(?, ?)");
        for(int i = 1; i <= 10; i++) {
            pstmt.setInt(1, i);
            pstmt.setCharacterStream(2, new StringReader(clobdata), clobdata.length());
            pstmt.executeUpdate();
        }
        pstmt.close();

        Statement stmtOrig = _connOther.createStatement();
        Statement stmtVJdbc = _connVJdbc.createStatement();
        ResultSet rsOrig = stmtOrig.executeQuery("select * from clobs");
        ResultSet rsVJdbc = stmtVJdbc.executeQuery("select * from clobs");
        compareResultSets(rsOrig, rsVJdbc);
        stmtOrig.close();
        stmtVJdbc.close();
    }
}
