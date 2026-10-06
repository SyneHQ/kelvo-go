// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.security.cert.*;
import java.sql.*;
import java.util.*;
import javax.net.ssl.*;

final class Profiles implements AutoCloseable {
    final String engine, host, database, schema, serverName;
    final int port;
    final SSLContext tls;
    private Path trustStore;
    private final Protocol.Input input;

    Profiles(Protocol.Input input) throws Exception {
        this.input=input;
        JsonNode source=input.source();
        engine=Protocol.text(source,"engine",20);
        if (!Set.of("h2","hive","spark","db2","sap_hana","sap_ase").contains(engine)) throw Protocol.unsupported();
        if (!Protocol.optional(source,"dsn",65536).isEmpty() || !Protocol.optional(source,"token",65536).isEmpty()) throw Protocol.invalid();
        URI uri=new URI(Protocol.text(source,"url",1024));
        host=uri.getHost(); port=uri.getPort();
        if (!"tls".equals(uri.getScheme()) || host==null || host.isEmpty() || host.length()>253 || port<1 || port>65535 || uri.getRawUserInfo()!=null || uri.getRawQuery()!=null || uri.getRawFragment()!=null || !uri.getRawPath().isEmpty() || host.contains("%")) throw Protocol.invalid();
        database=Protocol.text(source,"database",128); schema=Protocol.optional(source,"schema",128);
        if (!identifier(database,false) || !identifier(schema,true)) throw Protocol.invalid();
        if ((engine.equals("hive")||engine.equals("spark"))&&!schema.isEmpty()&&!schema.equals(database))throw Protocol.invalid();
        Protocol.text(source,"username",32768); Protocol.text(source,"password",32768);
        JsonNode options=source.path("options");
        if (!options.isMissingNode()) Protocol.keys(options,"tls_ca_pem","tls_server_name");
        String selected=Protocol.optional(options,"tls_server_name",253);
        serverName=selected.isEmpty()?host:selected;
        if (!serverName.matches("[A-Za-z0-9_.:\\[\\]-]{1,253}")) throw Protocol.invalid();
        if (!serverName.equals(host)) throw Protocol.unsupported();
        String pem=Protocol.optional(options,"tls_ca_pem",65536);
        TrustManagerFactory managers=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore store=null;
        if (!pem.isEmpty()) {
            store=KeyStore.getInstance("PKCS12");store.load(null,null);
            Collection<? extends java.security.cert.Certificate> certs=CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem.getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
            if (certs.isEmpty()) throw Protocol.invalid();
            int index=0;for (var certificate:certs) store.setCertificateEntry("ca-"+index++,certificate);
        }
        managers.init(store);tls=SSLContext.getInstance("TLS");tls.init(null,managers.getTrustManagers(),null);
        // One process per operation: no global TLS configuration is shared
        // between tenants. Only public CA certificates may touch scratch.
        SSLContext.setDefault(tls);
        if (store!=null && !engine.equals("h2")) {
            trustStore=Files.createTempFile(Path.of("."),"kelvo-trust-",".p12");
            Files.setPosixFilePermissions(trustStore,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
            try(OutputStream output=Files.newOutputStream(trustStore)){store.store(output,"kelvo-public-ca".toCharArray());}
        }
    }

    static boolean identifier(String value,boolean optional) {return optional&&value.isEmpty() || value.matches("[A-Za-z0-9_-]{1,128}");}
    String authority(){return (host.contains(":")&&!host.startsWith("[")?"["+host+"]":host)+":"+port;}
    boolean transactions(){return engine.equals("h2")||engine.equals("db2");}

    Connection open() throws Exception {
        if(System.currentTimeMillis()>=input.credentialsMillis())throw Protocol.invalid();
        Properties properties=new Properties();properties.setProperty("user",Protocol.text(input.source(),"username",32768));properties.setProperty("password",Protocol.text(input.source(),"password",32768));
        String driverClass,url;
        switch(engine){
            case "h2" -> {
                driverClass="org.h2.Driver";
                H2SocketFactory.install(this);
                Security.setProperty("ssl.SocketFactory.provider",H2SocketFactory.class.getName());
                // Prevent H2 from writing its built-in demonstration keystore.
                System.setProperty("javax.net.ssl.keyStore","NONE");
                System.setProperty("h2.socketConnectTimeout","5000");
                System.setProperty("h2.socketConnectRetry","0");
                url="jdbc:h2:ssl://"+authority()+"/"+database+";IFEXISTS=TRUE";
            }
            case "hive","spark" -> {
                driverClass="org.apache.hive.jdbc.HiveDriver";
                url="jdbc:hive2://"+authority()+"/"+database+";ssl=true;transportMode=binary";
                if(trustStore!=null)url+=";sslTrustStore="+trustStore.toAbsolutePath()+";trustStorePassword=kelvo-public-ca;trustStoreType=PKCS12";
            }
            case "db2" -> {
                driverClass="com.ibm.db2.jcc.DB2Driver";url="jdbc:db2://"+authority()+"/"+database;
                properties.setProperty("sslConnection","true");properties.setProperty("sslClientHostnameValidation","BASIC");
                properties.setProperty("loginTimeout","5");properties.setProperty("blockingReadConnectionTimeout","30");
                if(trustStore!=null){properties.setProperty("sslTrustStoreLocation",trustStore.toAbsolutePath().toString());properties.setProperty("sslTrustStorePassword","kelvo-public-ca");properties.setProperty("sslTrustStoreType","PKCS12");}
            }
            case "sap_hana" -> {
                driverClass="com.sap.db.jdbc.Driver";url="jdbc:sap://"+authority()+"/";
                properties.setProperty("databaseName",database);properties.setProperty("encrypt","true");properties.setProperty("validateCertificate","true");properties.setProperty("hostNameInCertificate",serverName);
                properties.setProperty("connectTimeout","5000");properties.setProperty("communicationTimeout","30000");
                if(trustStore!=null){properties.setProperty("trustStore",trustStore.toAbsolutePath().toString());properties.setProperty("trustStorePassword","kelvo-public-ca");properties.setProperty("trustStoreType","PKCS12");}
            }
            case "sap_ase" -> {
                driverClass="com.sybase.jdbc4.jdbc.SybDriver";url="jdbc:sybase:Tds:"+authority()+"/"+database;
                properties.setProperty("SYBSOCKET_FACTORY",AseSocketFactory.install(this));properties.setProperty("SSL_TRUST_ALL_CERTS","false");properties.setProperty("LOGIN_TIMEOUT","5");
            }
            default -> throw Protocol.unsupported();
        }
        Driver driver=(Driver)Class.forName(driverClass).getDeclaredConstructor().newInstance();
        if(System.currentTimeMillis()>=input.credentialsMillis())throw Protocol.invalid();
        Connection connection;
        try {connection=driver.connect(url,properties);}finally{properties.clear();}
        if(connection==null)throw Protocol.unsupported();
        boolean ready=false;
        try {
            if(System.currentTimeMillis()>=input.credentialsMillis())throw new SQLTimeoutException();
            if(!connection.getAutoCommit())throw Protocol.unsupported();
            if(!schema.isEmpty()&&!engine.equals("hive")&&!engine.equals("spark")) {connection.setSchema(schema);if(!schema.equals(connection.getSchema()))throw Protocol.unsupported();}
            ready=true;return connection;
        }finally{if(!ready)connection.close();}
    }
    public void close() throws IOException {if(trustStore!=null)Files.deleteIfExists(trustStore);}
}
