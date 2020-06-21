// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SerialResultSetMetaData implements ResultSetMetaData, Externalizable {
    static final long serialVersionUID = 9034215340975782405L;

    private int _columnCount;

    private String[] _catalogName;
    private String[] _schemaName;
    private String[] _tableName;
    private String[] _columnClassName;
    private String[] _columnLabel;
    private String[] _columnName;
    private String[] _columnTypeName;

    private Integer[] _columnType;
    private Integer[] _columnDisplaySize;
    private Integer[] _precision;
    private Integer[] _scale;
    private Integer[] _nullable;

    private Boolean[] _autoIncrement;
    private Boolean[] _caseSensitive;
    private Boolean[] _currency;
    private Boolean[] _readOnly;
    private Boolean[] _searchable;
    private Boolean[] _signed;
    private Boolean[] _writable;
    private Boolean[] _definitivelyWritable;

    public SerialResultSetMetaData() {
    }
    
    public SerialResultSetMetaData(ResultSetMetaData rsmd) throws SQLException {
        _columnCount = rsmd.getColumnCount();

        allocateArrays();
        fillArrays(rsmd);
    }
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _columnCount = in.readInt();

        _catalogName = (String[])in.readObject();
        _schemaName = (String[])in.readObject();
        _tableName = (String[])in.readObject();
        _columnClassName = (String[])in.readObject();
        _columnLabel = (String[])in.readObject();
        _columnName = (String[])in.readObject();
        _columnTypeName = (String[])in.readObject();

        _columnType = (Integer[])in.readObject();
        _columnDisplaySize = (Integer[])in.readObject();
        _precision = (Integer[])in.readObject();
        _scale = (Integer[])in.readObject();
        _nullable = (Integer[])in.readObject();

        _autoIncrement = (Boolean[])in.readObject();
        _caseSensitive = (Boolean[])in.readObject();
        _currency = (Boolean[])in.readObject();
        _readOnly = (Boolean[])in.readObject();
        _searchable = (Boolean[])in.readObject();
        _signed = (Boolean[])in.readObject();
        _writable = (Boolean[])in.readObject();
        _definitivelyWritable = (Boolean[])in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_columnCount);
        
        out.writeObject(_catalogName);
        out.writeObject(_schemaName);
        out.writeObject(_tableName);
        out.writeObject(_columnClassName);
        out.writeObject(_columnLabel);
        out.writeObject(_columnName);
        out.writeObject(_columnTypeName);

        out.writeObject(_columnType);
        out.writeObject(_columnDisplaySize);
        out.writeObject(_precision);
        out.writeObject(_scale);
        out.writeObject(_nullable);

        out.writeObject(_autoIncrement);
        out.writeObject(_caseSensitive);
        out.writeObject( _currency);
        out.writeObject(_readOnly);
        out.writeObject(_searchable);
        out.writeObject(_signed);
        out.writeObject(_writable);
        out.writeObject(_definitivelyWritable);
    }    

    private void allocateArrays() {
        _catalogName = new String[_columnCount];
        _schemaName = new String[_columnCount];
        _tableName = new String[_columnCount];
        _columnClassName = new String[_columnCount];
        _columnLabel = new String[_columnCount];
        _columnName = new String[_columnCount];
        _columnTypeName = new String[_columnCount];

        _columnDisplaySize = new Integer[_columnCount];
        _columnType = new Integer[_columnCount];
        _precision = new Integer[_columnCount];
        _scale = new Integer[_columnCount];
        _nullable = new Integer[_columnCount];

        _autoIncrement = new Boolean[_columnCount];
        _caseSensitive = new Boolean[_columnCount];
        _currency = new Boolean[_columnCount];
        _readOnly = new Boolean[_columnCount];
        _searchable = new Boolean[_columnCount];
        _signed = new Boolean[_columnCount];
        _writable = new Boolean[_columnCount];
        _definitivelyWritable = new Boolean[_columnCount];
    }

    private void fillArrays(ResultSetMetaData rsmd) {
        for(int i = 0; i < _columnCount; i++) {
            int col = i + 1;

            try {
                _catalogName[i] = rsmd.getCatalogName(col);
            } catch(Exception e) {
                _catalogName[i] = null;
            }

            try {
                _schemaName[i] = rsmd.getSchemaName(col);
            } catch(Exception e1) {
                _schemaName[i] = null;
            }

            try {
                _tableName[i] = rsmd.getTableName(col);
            } catch(Exception e2) {
                _tableName[i] = null;
            }

            try {
                _columnLabel[i] = rsmd.getColumnLabel(col);
            } catch(Exception e3) {
                _columnLabel[i] = null;
            }

            try {
                _columnName[i] = rsmd.getColumnName(col);
            } catch(Exception e4) {
                _columnName[i] = null;
            }

            try {
                _columnClassName[i] = rsmd.getColumnClassName(col);
            } catch(Exception e5) {
                _columnClassName[i] = null;
            }

            try {
                _columnTypeName[i] = rsmd.getColumnTypeName(col);
            } catch(Exception e6) {
                _columnTypeName[i] = null;
            }

            try {
                _columnDisplaySize[i] = new Integer(rsmd.getColumnDisplaySize(col));
            } catch(Exception e7) {
                _columnDisplaySize[i] = null;
            }

            try {
                _columnType[i] = new Integer(rsmd.getColumnType(col));
            } catch(Exception e8) {
                _columnType[i] = null;
            }

            try {
                _precision[i] = new Integer(rsmd.getPrecision(col));
            } catch(Exception e9) {
                _precision[i] = null;
            }

            try {
                _scale[i] = new Integer(rsmd.getScale(col));
            } catch(Exception e10) {
                _scale[i] = null;
            }

            try {
                _nullable[i] = new Integer(rsmd.isNullable(col));
            } catch(Exception e11) {
                _nullable[i] = null;
            }

            try {
                _autoIncrement[i] = rsmd.isAutoIncrement(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e12) {
                _autoIncrement[i] = null;
            }

            try {
                _caseSensitive[i] = rsmd.isCaseSensitive(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e13) {
                _caseSensitive[i] = null;
            }

            try {
                _currency[i] = rsmd.isCurrency(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e14) {
                _currency[i] = null;
            }

            try {
                _readOnly[i] = rsmd.isReadOnly(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e15) {
                _readOnly[i] = null;
            }

            try {
                _searchable[i] = rsmd.isSearchable(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e16) {
                _searchable[i] = null;
            }

            try {
                _signed[i] = rsmd.isSigned(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e17) {
                _signed[i] = null;
            }

            try {
                _writable[i] = rsmd.isWritable(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e18) {
                _writable[i] = null;
            }

            try {
                _definitivelyWritable[i] = rsmd.isDefinitelyWritable(col) ? Boolean.TRUE : Boolean.FALSE;
            } catch(Exception e18) {
                _definitivelyWritable[i] = null;
            }
        }
    }

    public int getColumnCount() throws SQLException {
        return _columnCount;
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        throwIfNull(_autoIncrement[column - 1]);
        return _autoIncrement[column - 1].booleanValue();
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        throwIfNull(_caseSensitive[column - 1]);
        return _caseSensitive[column - 1].booleanValue();
    }

    public boolean isSearchable(int column) throws SQLException {
        throwIfNull(_searchable[column - 1]);
        return _searchable[column - 1].booleanValue();
    }

    public boolean isCurrency(int column) throws SQLException {
        throwIfNull(_currency[column - 1]);
        return _currency[column - 1].booleanValue();
    }

    public int isNullable(int column) throws SQLException {
        throwIfNull(_nullable[column - 1]);
        return _nullable[column - 1].intValue();
    }

    public boolean isSigned(int column) throws SQLException {
        throwIfNull(_signed[column - 1]);
        return _signed[column - 1].booleanValue();
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        throwIfNull(_columnDisplaySize[column - 1]);
        return _columnDisplaySize[column - 1].intValue();
    }

    public String getColumnLabel(int column) throws SQLException {
        throwIfNull(_columnLabel[column - 1]);
        return _columnLabel[column - 1];
    }

    public String getColumnName(int column) throws SQLException {
        throwIfNull(_columnName[column - 1]);
        return _columnName[column - 1];
    }

    public String getSchemaName(int column) throws SQLException {
        throwIfNull(_schemaName[column - 1]);
        return _schemaName[column - 1];
    }

    public int getPrecision(int column) throws SQLException {
        throwIfNull(_precision[column - 1]);
        return _precision[column - 1].intValue();
    }

    public int getScale(int column) throws SQLException {
        throwIfNull(_scale[column - 1]);
        return _scale[column - 1].intValue();
    }

    public String getTableName(int column) throws SQLException {
        throwIfNull(_tableName[column - 1]);
        return _tableName[column - 1];
    }

    public String getCatalogName(int column) throws SQLException {
        throwIfNull(_catalogName[column - 1]);
        return _catalogName[column - 1];
    }

    public int getColumnType(int column) throws SQLException {
        throwIfNull(_columnType[column - 1]);
        return _columnType[column - 1].intValue();
    }

    public String getColumnTypeName(int column) throws SQLException {
        throwIfNull(_columnTypeName[column - 1]);
        return _columnTypeName[column - 1];
    }

    public boolean isReadOnly(int column) throws SQLException {
        throwIfNull(_readOnly[column - 1]);
        return _readOnly[column - 1].booleanValue();
    }

    public boolean isWritable(int column) throws SQLException {
        throwIfNull(_writable[column - 1]);
        return _writable[column - 1].booleanValue();
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        throwIfNull(_definitivelyWritable[column - 1]);
        return _definitivelyWritable[column - 1].booleanValue();
    }

    public String getColumnClassName(int column) throws SQLException {
        throwIfNull(_columnClassName[column - 1]);
        return _columnClassName[column - 1];
    }

    private void throwIfNull(Object obj) throws SQLException {
        if(obj == null) {
            throw new SQLException("Method not supported");
        }
    }
}
