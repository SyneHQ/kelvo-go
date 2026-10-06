package com.synehq.kelvo.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.lang.reflect.*;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.sql.*;
import java.util.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;

class WorkerTest {
    public interface SocketContract { java.net.Socket createSocket(String host,int port,Properties properties)throws IOException; }
    @Test void aseBridgeUsesVerifiedSocketContractAndFailsClosedWithoutSource()throws Exception{
        assertThrows(Exception.class,()->AseSocketFactory.define(Runnable.class));
        Class<?> generated=AseSocketFactory.define(SocketContract.class);
        SocketContract factory=(SocketContract)generated.getDeclaredConstructor().newInstance();
        assertThrows(IOException.class,()->factory.createSocket("localhost",1,new Properties()));
    }
    static String digest(String domain,String raw)throws Exception{MessageDigest hash=MessageDigest.getInstance("SHA-256");hash.update((domain+"\0").getBytes(StandardCharsets.UTF_8));hash.update(raw.getBytes(StandardCharsets.UTF_8));return HexFormat.of().formatHex(hash.digest());}
    static byte[] envelope(String request,String digest,long rows,long bytes)throws Exception{
        JsonNode r=Protocol.JSON.readTree(request);String database=r.path("connection").path("database").asText("");String schema=r.path("connection").path("schema").asText("");long now=System.currentTimeMillis()/1000;
        String source=Protocol.JSON.writeValueAsString(Map.of("engine","h2","url","tls://localhost:9092","username","reader","password","test-only","tenant_id","tenant-a","connection_id",r.path("connection").path("id").asText(),"revision","r1","database",database,"schema",schema));
        return ("{\"version\":1,\"operation_id\":\"operation-1\",\"request_sha256\":\""+digest+"\",\"request\":"+request+",\"source\":"+source+",\"limits\":{\"max_rows\":"+rows+",\"max_bytes\":"+bytes+",\"batch_rows\":16,\"timeout_ms\":10000},\"credentials_valid_until\":"+(now+5)+",\"expires_at\":"+(now+30)+",\"runtime\":{\"version\":1,\"java_fd\":7,\"jar_fds\":[8],\"heap_mb\":96,\"direct_mb\":32}}").getBytes(StandardCharsets.UTF_8);
    }
    static Protocol.Input input(String request,long rows,long bytes)throws Exception{return Protocol.parse(envelope(request,digest("kelvo.database.operation.v1",request),rows,bytes),System.currentTimeMillis());}
    static String query(String sql)throws Exception{return "{\"version\":1,\"kind\":\"query.read\",\"connection\":{\"id\":\"saved\",\"database\":\"fixture\"},\"idempotency_key\":\"\",\"spec\":{\"query\":{\"sql\":"+Protocol.JSON.writeValueAsString(sql)+"}}}";}

    @Test void matchesGoCanonicalDigestsAndRejectsModifiedBytes()throws Exception{
        JsonNode vectors=Protocol.JSON.readTree(Files.readAllBytes(Path.of("../../operations/testdata/request-digests.json")));
        for(JsonNode vector:vectors){String raw=vector.path("canonical_json").asText();String expected=vector.path("sha256").asText();assertEquals(expected,digest("kelvo.database.operation.v1",raw));assertEquals(expected,Protocol.parse(envelope(raw,expected,100,1<<20),System.currentTimeMillis()).digest());
            assertThrows(IllegalArgumentException.class,()->Protocol.parse(envelope(raw.replaceFirst("\\{","{ "),expected,100,1<<20),System.currentTimeMillis()));
        }
        String raw=query("SELECT 1");byte[] encoded=envelope(raw,digest("kelvo.database.operation.v1",raw),100,1<<20);
        String duplicate=new String(encoded,StandardCharsets.UTF_8).replaceFirst("\\{","{\"version\":1,");assertThrows(Exception.class,()->Protocol.parse(duplicate.getBytes(StandardCharsets.UTF_8),System.currentTimeMillis()));
        String colon=new String(encoded,StandardCharsets.UTF_8).replace("operation-1","operation:1");assertThrows(Exception.class,()->Protocol.parse(colon.getBytes(StandardCharsets.UTF_8),System.currentTimeMillis()));
    }
    @Test void bindsLargeUnsignedIntegersAndDecimalsExactly()throws Exception{
        List<Object> values=new ArrayList<>();PreparedStatement statement=(PreparedStatement)Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{PreparedStatement.class},(p,m,args)->{if(m.getName().startsWith("set")){values.add(args[1]);return null;}throw new UnsupportedOperationException();});
        Parameters.bind(statement,Protocol.JSON.readTree("[{\"type\":\"uint64\",\"value\":18446744073709551615},{\"type\":\"decimal128\",\"value\":\"9007199254740993.01\"}]"));
        assertEquals(new BigDecimal("18446744073709551615"),values.get(0));assertEquals(new BigDecimal("9007199254740993.01"),values.get(1));
        assertThrows(Exception.class,()->Parameters.bind(statement,Protocol.JSON.readTree("[{\"type\":\"uint8\",\"value\":256}]")));
    }
    @Test void arrowPreservesNullDecimalIntegerTimestampAndEmptySchema()throws Exception{
        try(Connection db=DriverManager.getConnection("jdbc:h2:mem:arrow");Statement statement=db.createStatement()){
            Protocol.Input input=input(query("SELECT 1"),100,1<<20);ByteArrayOutputStream bytes=new ByteArrayOutputStream();
            try(ResultSet rows=statement.executeQuery("SELECT CAST(NULL AS VARCHAR) AS N, CAST(9007199254740993.01 AS DECIMAL(30,2)) AS D, CAST(9223372036854775807 AS BIGINT) AS I, TIMESTAMP '2026-10-06 12:34:56.123456789' AS T")){
                ArrowResults.Result result=ArrowResults.write(rows,input,bytes,0,-1,null);assertEquals(1,result.rows());assertEquals(bytes.size(),result.bytes());assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())),result.sha256());
            }
            try(RootAllocator allocator=new RootAllocator(4<<20);ArrowStreamReader reader=new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()),allocator)){
                assertTrue(reader.loadNextBatch());VectorSchemaRoot root=reader.getVectorSchemaRoot();assertTrue(root.getVector("N").isNull(0));assertEquals(new BigDecimal("9007199254740993.01"),root.getVector("D").getObject(0));assertEquals(Long.MAX_VALUE,((BigIntVector)root.getVector("I")).get(0));assertEquals(123456789,((TimeStampNanoVector)root.getVector("T")).get(0)%1_000_000_000L);assertFalse(reader.loadNextBatch());
            }
            bytes.reset();try(ResultSet rows=statement.executeQuery("SELECT CAST(1 AS INTEGER) AS I WHERE FALSE")){assertEquals(0,ArrowResults.write(rows,input,bytes,0,-1,null).rows());}
            try(RootAllocator allocator=new RootAllocator(4<<20);ArrowStreamReader reader=new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()),allocator)){assertEquals("I",reader.getVectorSchemaRoot().getSchema().getFields().getFirst().getName());assertFalse(reader.loadNextBatch());}
        }
    }
    @Test void rowAndWireLimitsFailInsteadOfTruncating()throws Exception{
        try(Connection db=DriverManager.getConnection("jdbc:h2:mem:limits");Statement statement=db.createStatement()){
            try(ResultSet rows=statement.executeQuery("SELECT X FROM SYSTEM_RANGE(1,20)")){assertThrows(ArrowResults.Limit.class,()->ArrowResults.write(rows,input(query("SELECT 1"),10,1<<20),new ByteArrayOutputStream(),0,-1,null));}
            try(ResultSet rows=statement.executeQuery("SELECT REPEAT('a',1000) AS S")){assertThrows(ArrowResults.Limit.class,()->ArrowResults.write(rows,input(query("SELECT 1"),10,100),new ByteArrayOutputStream(),0,-1,null));}
        }
    }
    @Test void syntaxAndProfilesRejectControlStatementsAndUrlInjection()throws Exception{
        Execution.readSQL("WITH a AS (SELECT 1 AS n) SELECT * FROM a");
        for(String sql:List.of("SELECT 1; DROP TABLE data","SELECT CSVWRITE('x','y')","CALL 1","SELECT NEXT VALUE FOR seq"))assertThrows(Exception.class,()->Execution.readSQL(sql));
        for(String sql:List.of("COMMIT","ROLLBACK","SET SCHEMA other","CREATE ALIAS evil FOR 'Runtime.exec'","INSERT INTO t VALUES(1); DELETE FROM t"))assertThrows(Exception.class,()->Execution.changeSQL(sql,false));
        assertThrows(Exception.class,()->Execution.changeSQL("CREATE TABLE t(x INT)",true));
        Protocol.Input valid=input(query("SELECT 1"),10,1<<20);try(Profiles profile=new Profiles(valid)){assertEquals("h2",profile.engine);}
        for(String url:List.of("jdbc:h2:mem:private","tls://localhost:9092/../db","tls://user:pw@localhost:9092","tls://localhost:9092?ssl=false","http://localhost:9092")){
            var source=valid.source().deepCopy();((com.fasterxml.jackson.databind.node.ObjectNode)source).put("url",url);
            Protocol.Input changed=new Protocol.Input(valid.id(),valid.digest(),valid.request(),source,valid.rows(),valid.bytes(),valid.batchRows(),valid.deadlineMillis(),valid.credentialsMillis(),valid.directBytes(),valid.stepDigests());assertThrows(Exception.class,()->new Profiles(changed));
        }
    }
    @Test void redisIsRejectedBeforeLoadingAnyJdbcDriver()throws Exception{
        Protocol.Input valid=input(query("SELECT 1"),10,1<<20);
        var source=valid.source().deepCopy();((com.fasterxml.jackson.databind.node.ObjectNode)source).put("engine","redis");
        Protocol.Input changed=new Protocol.Input(valid.id(),valid.digest(),valid.request(),source,valid.rows(),valid.bytes(),valid.batchRows(),valid.deadlineMillis(),valid.credentialsMillis(),valid.directBytes(),valid.stepDigests());
        assertThrows(Exception.class,()->new Profiles(changed));
    }
    @Test void batchFailureRollsBackRequiredAndMarksAutocommitUnknown()throws Exception{
        for(String mode:List.of("required","autocommit")){
            String first="{\"sql\":\"INSERT INTO ITEMS VALUES(1)\"}", second="{\"sql\":\"INSERT INTO MISSING_TABLE VALUES(2)\"}";
            String request="{\"version\":1,\"kind\":\"statement.execute\",\"connection\":{\"id\":\"saved\",\"database\":\"fixture\"},\"idempotency_key\":\"write-1\",\"spec\":{\"statement\":{\"sql\":\"\",\"transaction\":\""+mode+"\",\"batch\":{\"statements\":["+first+","+second+"]}}}}";
            Protocol.Input input=input(request,100,1<<20);assertEquals(List.of(digest("kelvo.operation.statement.v1",first),digest("kelvo.operation.statement.v1",second)),input.stepDigests());
            try(Profiles profile=new Profiles(input);Connection db=DriverManager.getConnection("jdbc:h2:mem:batch_"+mode);Statement statement=db.createStatement()){
                statement.execute("CREATE TABLE ITEMS(ID INT)");Map<String,Object> receipt=Execution.run(input,profile,db,new ByteArrayOutputStream());
                assertEquals(mode.equals("required")?"failed":"failed",receipt.get("outcome"));
                // H2 rejects the missing table during prepare, before dispatch.
                assertEquals(mode.equals("required")?"none":"partial",receipt.get("effect"));
                try(ResultSet rows=statement.executeQuery("SELECT COUNT(*) FROM ITEMS")){assertTrue(rows.next());assertEquals(mode.equals("required")?0:1,rows.getInt(1));}
                assertEquals(2,((List<?>)receipt.get("steps")).size());
            }
        }
    }
}
