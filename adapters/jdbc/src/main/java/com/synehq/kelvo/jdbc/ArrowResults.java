// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import java.io.*;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;

final class ArrowResults {
    record Result(long rows,long bytes,String sha256) {}
    record Column(int index,String name,ArrowType type,int jdbc) {}
    static final class Limit extends IOException {}
    static final class Bounded extends OutputStream {
        final OutputStream output;final MessageDigest hash;final long maximum;long bytes;
        Bounded(OutputStream output,long maximum)throws Exception {this.output=output;this.maximum=maximum;hash=MessageDigest.getInstance("SHA-256");}
        public void write(int value)throws IOException{write(new byte[]{(byte)value});}
        public void write(byte[] value,int offset,int length)throws IOException{if(length>maximum-bytes)throw new Limit();output.write(value,offset,length);hash.update(value,offset,length);bytes+=length;}
        public void close()throws IOException{output.flush();}
    }

    static Result write(ResultSet rows,Protocol.Input input,OutputStream output,long skip,int pageSize,Map<String,String> selected)throws Exception{
        ResultSetMetaData metadata=rows.getMetaData();List<Column> columns=new ArrayList<>();List<Field> fields=new ArrayList<>();Set<String> names=new HashSet<>();
        if(metadata.getColumnCount()<1||metadata.getColumnCount()>4096)throw Protocol.unsupported();
        for(int index=1;index<=metadata.getColumnCount();index++){
            String label=metadata.getColumnLabel(index);if(selected!=null&&!selected.containsKey(label))continue;
            String name=selected==null?label:selected.get(label);
            if(name==null||name.isEmpty()||name.length()>1024||!names.add(name))throw Protocol.unsupported();
            ArrowType type=type(metadata,index);columns.add(new Column(index,name,type,metadata.getColumnType(index)));fields.add(Field.nullable(name,type));
        }
        if(columns.isEmpty())throw Protocol.unsupported();
        Bounded wire=new Bounded(output,input.bytes());long count=0;int batch=0;long batchBytes=0;
        try(RootAllocator allocator=new RootAllocator(input.directBytes());VectorSchemaRoot root=VectorSchemaRoot.create(new Schema(fields),allocator);ArrowStreamWriter writer=new ArrowStreamWriter(root,null,wire)){
            for(FieldVector vector:root.getFieldVectors())vector.setInitialCapacity(Math.min(input.batchRows(),64));root.allocateNew();writer.start();
            while(skip>0&&rows.next()){checkDeadline(input);skip--;}
            while((pageSize<0||count<pageSize)&&rows.next()){
                checkDeadline(input);if(count>=input.rows())throw new Limit();
                for(int i=0;i<columns.size();i++)batchBytes+=value(rows,columns.get(i),root.getVector(i),batch,input.bytes()-batchBytes);
                batch++;count++;
                if(batch>=input.batchRows()||batchBytes>=Math.min(1<<20,input.directBytes()/4)){
                    root.setRowCount(batch);writer.writeBatch();for(FieldVector vector:root.getFieldVectors())vector.reset();batch=0;batchBytes=0;
                }
            }
            if(batch>0){root.setRowCount(batch);writer.writeBatch();}writer.end();
        }
        return new Result(count,wire.bytes,HexFormat.of().formatHex(wire.hash.digest()));
    }
    static void checkDeadline(Protocol.Input input)throws SQLTimeoutException{if(System.currentTimeMillis()>=input.deadlineMillis())throw new SQLTimeoutException();}
    static ArrowType type(ResultSetMetaData metadata,int i)throws SQLException{
        int jdbc=metadata.getColumnType(i);
        return switch(jdbc){
            case Types.NULL -> ArrowType.Null.INSTANCE;
            case Types.BIT,Types.BOOLEAN -> ArrowType.Bool.INSTANCE;
            case Types.TINYINT -> new ArrowType.Int(8,metadata.isSigned(i));
            case Types.SMALLINT -> new ArrowType.Int(16,metadata.isSigned(i));
            case Types.INTEGER -> new ArrowType.Int(32,metadata.isSigned(i));
            case Types.BIGINT -> new ArrowType.Int(64,metadata.isSigned(i));
            case Types.REAL -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case Types.FLOAT,Types.DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case Types.DECIMAL,Types.NUMERIC -> {int precision=metadata.getPrecision(i),scale=metadata.getScale(i);if(precision<1||precision>76||scale>precision||scale< -precision)throw Protocol.unsupported();yield new ArrowType.Decimal(precision,scale,precision<=38?128:256);}
            case Types.CHAR,Types.VARCHAR,Types.LONGVARCHAR,Types.NCHAR,Types.NVARCHAR,Types.LONGNVARCHAR,Types.CLOB,Types.NCLOB -> ArrowType.Utf8.INSTANCE;
            case Types.BINARY,Types.VARBINARY,Types.LONGVARBINARY,Types.BLOB -> ArrowType.Binary.INSTANCE;
            case Types.DATE -> new ArrowType.Date(DateUnit.DAY);
            case Types.TIME -> new ArrowType.Time(TimeUnit.NANOSECOND,64);
            case Types.TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.NANOSECOND,null);
            case Types.TIMESTAMP_WITH_TIMEZONE -> new ArrowType.Timestamp(TimeUnit.NANOSECOND,"UTC");
            default -> throw Protocol.unsupported();
        };
    }
    static long value(ResultSet rows,Column column,FieldVector vector,int row,long available)throws Exception{
        int index=column.index();
        // Probe null without materializing a LOB through getObject.
        if(column.jdbc()==Types.NULL){vector.setNull(row);return 1;}
        if(column.type() instanceof ArrowType.Utf8){
            try(Reader stream=rows.getCharacterStream(index)){
                if(stream==null){vector.setNull(row);return 1;}
                StringBuilder text=new StringBuilder();char[] buffer=new char[2048];int n;long cap=Math.min(available,16<<20);while((n=stream.read(buffer))!=-1){if(text.length()+n>cap)throw new Limit();text.append(buffer,0,n);}
                byte[] value=text.toString().getBytes(StandardCharsets.UTF_8);if(value.length>available)throw new Limit();((VarCharVector)vector).setSafe(row,value);return value.length+5;
            }
        }
        if(column.type() instanceof ArrowType.Binary){
            try(InputStream stream=rows.getBinaryStream(index)){
                if(stream==null){vector.setNull(row);return 1;}
                int cap=(int)Math.min(Math.min(available,16<<20),Integer.MAX_VALUE-1);if(cap<0)throw new Limit();byte[] value=stream.readNBytes(cap+1);if(value.length>cap)throw new Limit();((VarBinaryVector)vector).setSafe(row,value);return value.length+5;
            }
        }
        if(column.type() instanceof ArrowType.Decimal type){BigDecimal number=rows.getBigDecimal(index);if(number==null){vector.setNull(row);return 1;}number=number.setScale(type.getScale(),RoundingMode.UNNECESSARY);if(number.precision()>type.getPrecision())throw Protocol.unsupported();if(vector instanceof DecimalVector v)v.setSafe(row,number);else ((Decimal256Vector)vector).setSafe(row,number);return type.getBitWidth()/8+1;}
        Object value=rows.getObject(index);if(value==null){vector.setNull(row);return 1;}
        if(column.type() instanceof ArrowType.Int type){
            BigInteger number=rows.getBigDecimal(index).toBigIntegerExact();int bits=type.getBitWidth();BigInteger low=type.getIsSigned()?BigInteger.ONE.shiftLeft(bits-1).negate():BigInteger.ZERO,high=BigInteger.ONE.shiftLeft(type.getIsSigned()?bits-1:bits).subtract(BigInteger.ONE);if(number.compareTo(low)<0||number.compareTo(high)>0)throw Protocol.unsupported();
            if(vector instanceof TinyIntVector v)v.setSafe(row,number.byteValueExact());else if(vector instanceof SmallIntVector v)v.setSafe(row,number.shortValueExact());else if(vector instanceof IntVector v)v.setSafe(row,number.intValueExact());else if(vector instanceof BigIntVector v)v.setSafe(row,number.longValueExact());else if(vector instanceof UInt1Vector v)v.setSafe(row,number.intValue());else if(vector instanceof UInt2Vector v)v.setSafe(row,number.intValue());else if(vector instanceof UInt4Vector v)v.setSafe(row,number.intValue());else if(vector instanceof UInt8Vector v)v.setSafe(row,number.longValue());else throw Protocol.unsupported();return bits/8+1;
        }
        if(vector instanceof BitVector v){v.setSafe(row,rows.getBoolean(index)?1:0);return 2;}
        if(vector instanceof Float4Vector v){float number=rows.getFloat(index);if(!Float.isFinite(number))throw Protocol.unsupported();v.setSafe(row,number);return 5;}
        if(vector instanceof Float8Vector v){double number=rows.getDouble(index);if(!Double.isFinite(number))throw Protocol.unsupported();v.setSafe(row,number);return 9;}
        if(vector instanceof DateDayVector v){v.setSafe(row,Math.toIntExact(rows.getObject(index,LocalDate.class).toEpochDay()));return 5;}
        if(vector instanceof TimeNanoVector v){v.setSafe(row,rows.getObject(index,LocalTime.class).toNanoOfDay());return 9;}
        if(column.type() instanceof ArrowType.Timestamp type){Instant instant=type.getTimezone()==null?rows.getObject(index,LocalDateTime.class).toInstant(ZoneOffset.UTC):rows.getObject(index,OffsetDateTime.class).toInstant();long nanos=Math.addExact(Math.multiplyExact(instant.getEpochSecond(),1_000_000_000L),instant.getNano());((TimeStampVector)vector).setSafe(row,nanos);return 9;}
        throw Protocol.unsupported();
    }
}
