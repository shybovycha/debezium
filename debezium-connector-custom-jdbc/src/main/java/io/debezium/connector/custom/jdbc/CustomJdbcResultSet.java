/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.custom.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import io.debezium.relational.Table;

/**
 * A decorator for a {@link ResultSet} that cancels the parent {@link Statement} before the delegate set is closed.
 * Some JDBC drivers need to cycle through all the fetched rows before they can close a result set, so for potentially
 * large queries such as table snapshots, a round trip to the server to cancel the statement could save time.
 *
 * @author Mark Bereznitsky
 */
public class CustomJdbcResultSet implements ResultSet {

    private final ResultSet resultSet;

    private final Map<String, Integer> columnIndexMap;

    private CustomJdbcResultSet(ResultSet resultSet, Table table) {
        this.resultSet = resultSet;

        this.columnIndexMap = new LinkedHashMap<>();

        for (int i = 0; i < table.columns().size(); ++i) {
            columnIndexMap.put(table.columns().get(i).name(), i);
        }
    }

    public static CustomJdbcResultSet from(ResultSet resultSet, Table table) {
        return new CustomJdbcResultSet(resultSet, table);
    }

    @Override
    public boolean next() throws SQLException {
        return resultSet.next();
    }

    @Override
    public void close() throws SQLException {
        try {
            if (!isClosed() && !isAfterLast()) { // Result set is still open and not exhausted
                getStatement().cancel();
            }
        }
        catch (Exception ignored) {
        }

        resultSet.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return resultSet.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return resultSet.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return resultSet.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return resultSet.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return resultSet.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return resultSet.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return resultSet.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return resultSet.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return resultSet.getDouble(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnIndex, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return resultSet.getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return resultSet.getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return resultSet.getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return resultSet.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return resultSet.getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return resultSet.getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return resultSet.getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return resultSet.getString(columnIndexMap.get(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return resultSet.getBoolean(columnIndexMap.get(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return resultSet.getByte(columnIndexMap.get(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return resultSet.getShort(columnIndexMap.get(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return resultSet.getInt(columnIndexMap.get(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return resultSet.getLong(columnIndexMap.get(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return resultSet.getFloat(columnIndexMap.get(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return resultSet.getDouble(columnIndexMap.get(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnIndexMap.get(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return resultSet.getBytes(columnIndexMap.get(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return resultSet.getDate(columnIndexMap.get(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return resultSet.getTime(columnIndexMap.get(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return resultSet.getTimestamp(columnIndexMap.get(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return resultSet.getAsciiStream(columnIndexMap.get(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return resultSet.getUnicodeStream(columnIndexMap.get(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return resultSet.getBinaryStream(columnIndexMap.get(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return resultSet.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        resultSet.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return resultSet.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSet.getMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return resultSet.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return resultSet.getObject(columnIndexMap.get(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return columnIndexMap.get(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return resultSet.getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return resultSet.getCharacterStream(columnIndexMap.get(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return resultSet.getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return resultSet.getBigDecimal(columnIndexMap.get(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return resultSet.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return resultSet.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return resultSet.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return resultSet.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        resultSet.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        resultSet.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return resultSet.first();
    }

    @Override
    public boolean last() throws SQLException {
        return resultSet.last();
    }

    @Override
    public int getRow() throws SQLException {
        return resultSet.getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return resultSet.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return resultSet.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return resultSet.previous();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        resultSet.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return resultSet.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        resultSet.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return resultSet.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return resultSet.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return resultSet.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return resultSet.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return resultSet.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return resultSet.rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        resultSet.updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        resultSet.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        resultSet.updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        resultSet.updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        resultSet.updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        resultSet.updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        resultSet.updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        resultSet.updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        resultSet.updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        resultSet.updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        resultSet.updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        resultSet.updateDate(columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        resultSet.updateTime(columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        resultSet.updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        resultSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        resultSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        resultSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        resultSet.updateObject(columnIndex, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        resultSet.updateObject(columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        resultSet.updateNull(columnIndexMap.get(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        resultSet.updateBoolean(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        resultSet.updateByte(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        resultSet.updateShort(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        resultSet.updateInt(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        resultSet.updateLong(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        resultSet.updateFloat(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        resultSet.updateDouble(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        resultSet.updateBigDecimal(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        resultSet.updateString(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        resultSet.updateBytes(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        resultSet.updateDate(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        resultSet.updateTime(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        resultSet.updateTimestamp(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        resultSet.updateAsciiStream(columnIndexMap.get(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        resultSet.updateBinaryStream(columnIndexMap.get(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        resultSet.updateCharacterStream(columnIndexMap.get(columnLabel), reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        resultSet.updateObject(columnIndexMap.get(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        resultSet.updateObject(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void insertRow() throws SQLException {
        resultSet.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        resultSet.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        resultSet.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        resultSet.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        resultSet.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        resultSet.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        resultSet.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return resultSet.getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return resultSet.getObject(columnIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return resultSet.getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return resultSet.getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return resultSet.getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return resultSet.getArray(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return resultSet.getObject(columnIndexMap.get(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return resultSet.getRef(columnIndexMap.get(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return resultSet.getBlob(columnIndexMap.get(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return resultSet.getClob(columnIndexMap.get(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return resultSet.getArray(columnIndexMap.get(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return resultSet.getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return resultSet.getDate(columnIndexMap.get(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return resultSet.getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return resultSet.getTime(columnIndexMap.get(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return resultSet.getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return resultSet.getTimestamp(columnIndexMap.get(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return resultSet.getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return resultSet.getURL(columnIndexMap.get(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        resultSet.updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        resultSet.updateRef(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        resultSet.updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        resultSet.updateBlob(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        resultSet.updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        resultSet.updateClob(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        resultSet.updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        resultSet.updateArray(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return resultSet.getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return resultSet.getRowId(columnIndexMap.get(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        resultSet.updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        resultSet.updateRowId(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return resultSet.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return resultSet.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        resultSet.updateNString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        resultSet.updateNString(columnIndexMap.get(columnLabel), nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        resultSet.updateNClob(columnIndex, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        resultSet.updateNClob(columnIndexMap.get(columnLabel), nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return resultSet.getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return resultSet.getNClob(columnIndexMap.get(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return resultSet.getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return resultSet.getSQLXML(columnIndexMap.get(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        resultSet.updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        resultSet.updateSQLXML(columnIndexMap.get(columnLabel), xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return resultSet.getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return resultSet.getNString(columnIndexMap.get(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return resultSet.getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return resultSet.getNCharacterStream(columnIndexMap.get(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        resultSet.updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateNCharacterStream(columnIndexMap.get(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        resultSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        resultSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        resultSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        resultSet.updateAsciiStream(columnIndexMap.get(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        resultSet.updateBinaryStream(columnIndexMap.get(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateCharacterStream(columnIndexMap.get(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        resultSet.updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        resultSet.updateBlob(columnIndexMap.get(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        resultSet.updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateClob(columnIndexMap.get(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        resultSet.updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateNClob(columnIndexMap.get(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        resultSet.updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateNCharacterStream(columnIndexMap.get(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        resultSet.updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        resultSet.updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        resultSet.updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        resultSet.updateAsciiStream(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        resultSet.updateBinaryStream(columnIndexMap.get(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateCharacterStream(columnIndexMap.get(columnLabel), reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        resultSet.updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        resultSet.updateBlob(columnIndexMap.get(columnLabel), inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        resultSet.updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateClob(columnIndexMap.get(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        resultSet.updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateNClob(columnIndexMap.get(columnLabel), reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return resultSet.getObject(columnIndex, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return resultSet.getObject(columnIndexMap.get(columnLabel), type);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        resultSet.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        resultSet.updateObject(columnIndexMap.get(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        resultSet.updateObject(columnIndex, x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        resultSet.updateObject(columnIndexMap.get(columnLabel), x, targetSqlType);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return resultSet.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return resultSet.isWrapperFor(iface);
    }
}
