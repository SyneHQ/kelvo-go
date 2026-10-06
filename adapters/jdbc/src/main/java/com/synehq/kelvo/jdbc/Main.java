// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public final class Main {
    public static void main(String[] args){
        int code=1;
        // Preserve protocol stdout before suppressing driver debug output.
        OutputStream output=System.out;System.setOut(new PrintStream(OutputStream.nullOutputStream()));System.setErr(new PrintStream(OutputStream.nullOutputStream()));
        try(OutputStream receipt=new FileOutputStream("/proc/self/fd/3")){
            if(args.length!=0)throw Protocol.invalid();Protocol.Input input=Protocol.read(System.in);Map<String,Object> result=Execution.initial(input);
            ScheduledExecutorService watchdog=Executors.newSingleThreadScheduledExecutor(r->{Thread thread=new Thread(r,"kelvo-deadline");thread.setDaemon(true);return thread;});
            watchdog.schedule(()->Runtime.getRuntime().halt(124),Math.max(1,input.deadlineMillis()-System.currentTimeMillis()),TimeUnit.MILLISECONDS);
            try(Profiles profile=new Profiles(input)){
                ExecutorService opening=Executors.newSingleThreadExecutor(r->{Thread thread=new Thread(r,"kelvo-open");thread.setDaemon(true);return thread;});
                Future<Connection> pending=opening.submit(profile::open);
                try(Connection connection=pending.get(Math.max(1,input.credentialsMillis()-System.currentTimeMillis()),TimeUnit.MILLISECONDS)) {result=Execution.run(input,profile,connection,output);}
                finally{pending.cancel(true);opening.shutdownNow();}
            }catch(Exception|LinkageError failure){if(result.get("outcome").equals("rejected"))result.put("error_code",failure instanceof UnsupportedOperationException?"UNSUPPORTED":"SOURCE_FAILED");}
            byte[] encoded=Protocol.JSON.writeValueAsBytes(result);if(encoded.length>32768)throw Protocol.invalid();receipt.write(encoded);receipt.flush();watchdog.shutdownNow();code=result.get("outcome").equals("completed")?0:1;
        }catch(Exception|LinkageError failure){}
        System.exit(code);
    }
}
