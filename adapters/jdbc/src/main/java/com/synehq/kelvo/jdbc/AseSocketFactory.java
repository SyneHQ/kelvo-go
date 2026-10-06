// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import javax.net.ssl.*;

// The vendor owns the interface. A fixed two-method bridge forwards socket
// creation to our TLS verifier without bundling vendor classes or a compiler.
public final class AseSocketFactory {
    private static Profiles source;
    static String install(Profiles profile)throws Exception{
        source=profile;Class<?> contract=null;
        for(String name:List.of("com.sybase.jdbc4.jdbcx.SybSocketFactory","com.sybase.jdbcx.SybSocketFactory","com.sybase.jdbc4.jdbc.SybSocketFactory")){
            try{contract=Class.forName(name);break;}catch(ClassNotFoundException ignored){}
        }
        if(contract==null)throw Protocol.unsupported();return define(contract).getName();
    }
    static Class<?> define(Class<?> contract)throws Exception{
        if(!contract.isInterface()||!Modifier.isPublic(contract.getModifiers()))throw Protocol.unsupported();
        Method socket=contract.getMethod("createSocket",String.class,int.class,Properties.class);
        if(socket.getReturnType()!=Socket.class)throw Protocol.unsupported();
        for(Method method:contract.getMethods())if(Modifier.isAbstract(method.getModifiers())&&!method.equals(socket))throw Protocol.unsupported();
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);
        out.writeInt(0xcafebabe);out.writeShort(0);out.writeShort(65);out.writeShort(19);
        utf(out,"com/synehq/kelvo/jdbc/PinnedAseSocketFactory");clazz(out,1);utf(out,"java/lang/Object");clazz(out,3);utf(out,contract.getName().replace('.','/'));clazz(out,5);
        utf(out,"<init>");utf(out,"()V");pair(out,12,7,8);pair(out,10,4,9);utf(out,"Code");utf(out,"createSocket");
        utf(out,"(Ljava/lang/String;ILjava/util/Properties;)Ljava/net/Socket;");utf(out,"com/synehq/kelvo/jdbc/AseSocketFactory");clazz(out,14);utf(out,"open");pair(out,12,16,13);pair(out,10,15,17);
        out.writeShort(0x31);out.writeShort(2);out.writeShort(4);out.writeShort(1);out.writeShort(6);out.writeShort(0);out.writeShort(2);
        method(out,7,8,1,1,new byte[]{0x2a,(byte)0xb7,0,10,(byte)0xb1});
        method(out,12,13,3,4,new byte[]{0x2b,0x1c,0x2d,(byte)0xb8,0,18,(byte)0xb0});out.writeShort(0);out.flush();
        return MethodHandles.lookup().defineClass(bytes.toByteArray());
    }
    private static void utf(DataOutputStream out,String s)throws IOException{out.writeByte(1);out.writeUTF(s);}
    private static void clazz(DataOutputStream out,int name)throws IOException{out.writeByte(7);out.writeShort(name);}
    private static void pair(DataOutputStream out,int tag,int a,int b)throws IOException{out.writeByte(tag);out.writeShort(a);out.writeShort(b);}
    private static void method(DataOutputStream out,int name,int descriptor,int stack,int locals,byte[] code)throws IOException{
        out.writeShort(1);out.writeShort(name);out.writeShort(descriptor);out.writeShort(1);out.writeShort(11);out.writeInt(12+code.length);out.writeShort(stack);out.writeShort(locals);out.writeInt(code.length);out.write(code);out.writeShort(0);out.writeShort(0);
    }
    public static Socket open(String host,int port,Properties ignored)throws IOException{
        Profiles expected=source;
        if(expected==null||!expected.host.equals(host)||expected.port!=port)throw new IOException("SOURCE_FAILED");
        SSLSocket socket=(SSLSocket)expected.tls.getSocketFactory().createSocket();boolean ready=false;
        try{
            SSLParameters parameters=socket.getSSLParameters();parameters.setEndpointIdentificationAlgorithm("HTTPS");parameters.setProtocols(new String[]{"TLSv1.3","TLSv1.2"});socket.setSSLParameters(parameters);
            socket.connect(new InetSocketAddress(host,port),5000);socket.setSoTimeout(30000);socket.startHandshake();ready=true;return socket;
        }finally{if(!ready)socket.close();}
    }
}
