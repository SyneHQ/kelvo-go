// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import java.io.IOException;
import java.net.*;
import javax.net.ssl.*;

// H2 uses JSSE's no-argument socket factory and otherwise omits hostname
// verification. A fixed provider enables the standard HTTPS identity check on
// every socket before the driver connects it to its constructed source URL.
public final class H2SocketFactory extends SSLSocketFactory {
    private static Profiles source;
    static void install(Profiles profile){source=profile;}
    private SSLSocketFactory factory()throws IOException{if(source==null)throw new IOException("SOURCE_FAILED");return source.tls.getSocketFactory();}
    private Socket checked(Socket socket)throws IOException{
        SSLSocket tls=(SSLSocket)socket;SSLParameters parameters=tls.getSSLParameters();parameters.setEndpointIdentificationAlgorithm("HTTPS");parameters.setProtocols(new String[]{"TLSv1.3","TLSv1.2"});tls.setSSLParameters(parameters);tls.setSoTimeout(30000);return tls;
    }
    public String[] getDefaultCipherSuites(){return source.tls.getSocketFactory().getDefaultCipherSuites();}
    public String[] getSupportedCipherSuites(){return source.tls.getSocketFactory().getSupportedCipherSuites();}
    public Socket createSocket()throws IOException{return checked(factory().createSocket());}
    public Socket createSocket(Socket socket,String host,int port,boolean close)throws IOException{return checked(factory().createSocket(socket,host,port,close));}
    public Socket createSocket(String host,int port)throws IOException{return checked(factory().createSocket(host,port));}
    public Socket createSocket(String host,int port,InetAddress local,int localPort)throws IOException{return checked(factory().createSocket(host,port,local,localPort));}
    public Socket createSocket(InetAddress host,int port)throws IOException{return checked(factory().createSocket(host,port));}
    public Socket createSocket(InetAddress host,int port,InetAddress local,int localPort)throws IOException{return checked(factory().createSocket(host,port,local,localPort));}
}
