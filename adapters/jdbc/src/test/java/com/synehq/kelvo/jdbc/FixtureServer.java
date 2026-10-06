package com.synehq.kelvo.jdbc;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.h2.tools.Server;

// Only the external VM harness invokes this test class. Configuration and
// random credentials arrive through stdin; stdout contains only the port.
public final class FixtureServer {
    public static void main(String[] args)throws Exception{
        if(args.length==1&&args[0].equals("inspect")){
            try(var allocator=new org.apache.arrow.memory.RootAllocator(16<<20);var reader=new org.apache.arrow.vector.ipc.ArrowStreamReader(System.in,allocator)){
                List<Map<String,String>> rows=new ArrayList<>();
                while(reader.loadNextBatch()){
                    var root=reader.getVectorSchemaRoot();
                    for(int i=0;i<root.getRowCount();i++) {Map<String,String> row=new LinkedHashMap<>();for(var column:root.getFieldVectors()){Object value=column.getObject(i);row.put(column.getName(),value==null?null:value.toString());}rows.add(row);}
                }
                System.out.print(Protocol.JSON.writeValueAsString(rows));
            }
            return;
        }

        System.setErr(new PrintStream(OutputStream.nullOutputStream()));
        var input=Protocol.JSON.readTree(System.in);String directory=input.path("directory").asText();String password=input.path("password").asText();
        if(!directory.startsWith("/dev/shm/kelvo-jdbc-live-")||!password.matches("[A-Za-z0-9_]{32,100}"))throw Protocol.invalid();
        System.setProperty("javax.net.ssl.keyStore",directory+"/server.p12");System.setProperty("javax.net.ssl.keyStorePassword",input.path("keystore_password").asText());System.setProperty("javax.net.ssl.keyStoreType","PKCS12");
        for(String database:List.of("fixture_a","fixture_b")){
            try(Connection connection=DriverManager.getConnection("jdbc:h2:"+directory+"/"+database,"SA",password);Statement statement=connection.createStatement()){
                statement.execute("CREATE USER APP_USER PASSWORD '"+password+"'");
                statement.execute("CREATE TABLE ITEMS(ID BIGINT PRIMARY KEY, AMOUNT DECIMAL(30,2))");
                statement.execute("INSERT INTO ITEMS VALUES(1,9007199254740993.01)");
                statement.execute("GRANT SELECT,INSERT,UPDATE,DELETE ON ITEMS TO APP_USER");
            }
        }
        Server server=Server.createTcpServer("-tcpPort","0","-tcpSSL","-baseDir",directory,"-ifExists").start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        System.out.println(server.getPort());System.out.flush();
        synchronized(FixtureServer.class){FixtureServer.class.wait();}
    }
}
