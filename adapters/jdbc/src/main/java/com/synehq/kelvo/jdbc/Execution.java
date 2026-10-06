// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.*;
import java.sql.*;
import java.util.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;

final class Execution {
    static Map<String,Object> initial(Protocol.Input input){Map<String,Object> r=new LinkedHashMap<>();r.put("version",1);r.put("operation_id",input.id());r.put("request_sha256",input.digest());r.put("outcome","rejected");r.put("effect","none");r.put("error_code","UNSUPPORTED");return r;}
    static void complete(Map<String,Object> receipt){receipt.put("outcome","completed");receipt.remove("error_code");}
    static void failed(Map<String,Object> receipt,String code){receipt.put("outcome","failed");receipt.put("error_code",code);}
    static void result(Map<String,Object> receipt,Protocol.Input input,ArrowResults.Result result){complete(receipt);receipt.put("result",Map.of("id",input.id(),"sha256",result.sha256(),"bytes",result.bytes(),"rows",result.rows(),"format","arrow_ipc"));}
    static void timeout(Statement statement,Protocol.Input input)throws SQLException{ArrowResults.checkDeadline(input);statement.setQueryTimeout((int)Math.max(1,(input.deadlineMillis()-System.currentTimeMillis()+999)/1000));}

    static Map<String,Object> run(Protocol.Input input,Profiles profile,Connection connection,OutputStream output){
        Map<String,Object> receipt=initial(input);
        try {
            String kind=Protocol.text(input.request(),"kind",40);JsonNode spec=input.request().path("spec");
            switch(kind){
                case "connection.test" -> {Protocol.keys(spec);if(!connection.isValid(3))throw new SQLException();complete(receipt);}
                case "query.read" -> {
                    Protocol.keys(spec,"query");JsonNode query=spec.path("query");Protocol.keys(query,"sql","parameters");String sql=Protocol.text(query,"sql",65536);readSQL(sql);
                    failed(receipt,"SOURCE_FAILED");
                    // Drivers differ in database-enforced read-only support;
                    // source privileges remain authoritative for functions.
                    if(profile.transactions())connection.setAutoCommit(false);
                    try(PreparedStatement statement=connection.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY)){
                        timeout(statement,input);statement.setFetchSize(input.batchRows());Parameters.bind(statement,query.path("parameters"));
                        try(ResultSet rows=statement.executeQuery()){result(receipt,input,ArrowResults.write(rows,input,output,0,-1,null));}
                    }finally{if(profile.transactions())connection.rollback();}
                }
                case "metadata.inspect" -> {
                    Protocol.keys(spec,"metadata");Metadata.Selection selection=Metadata.validate(input,spec.path("metadata"));failed(receipt,"SOURCE_FAILED");
                    try(ResultSet rows=Metadata.open(connection.getMetaData(),selection,profile)) {result(receipt,input,ArrowResults.write(rows,input,output,selection.offset(),selection.limit(),selection.columns()));}
                }
                case "statement.execute" -> {Protocol.keys(spec,"statement");change(input,profile,connection,spec.path("statement"),receipt);}
                default -> throw Protocol.unsupported();
            }
        }catch(UnsupportedOperationException|IllegalArgumentException e){if(receipt.get("outcome").equals("rejected"))receipt.put("error_code","UNSUPPORTED");else if(!receipt.get("outcome").equals("outcome_unknown"))failed(receipt,"SOURCE_FAILED");}
        catch(ArrowResults.Limit e){failed(receipt,"RESOURCE_EXHAUSTED");receipt.remove("result");}
        catch(Exception|LinkageError e){if(!receipt.get("outcome").equals("outcome_unknown")){failed(receipt,"SOURCE_FAILED");receipt.remove("result");}}
        return receipt;
    }

    static void readSQL(String sql)throws Exception {
        var statements=CCJSqlParserUtil.parseStatements(sql).getStatements();
        if(statements.size()!=1||!(statements.getFirst() instanceof Select))throw Protocol.unsupported();
        // Conservative H2/warehouse hazards. SQL permissions are still needed
        // for user-defined functions and fully-qualified source objects.
        if(java.util.regex.Pattern.compile("(?i)\\b(FILE_READ|FILE_WRITE|CSVWRITE|CSVREAD|LINK_SCHEMA|NEXTVAL|NEXT|OPENQUERY|OPENROWSET|INTO|FOR\\s+UPDATE)\\b").matcher(sql).find())throw Protocol.unsupported();
    }
    static String changeSQL(String sql,boolean transaction)throws Exception {
        var list=CCJSqlParserUtil.parseStatements(sql).getStatements();if(list.size()!=1)throw Protocol.unsupported();
        String type=list.getFirst().getClass().getSimpleName();Set<String> allowed=transaction?Set.of("Insert","Update","Delete","Merge"):Set.of("Insert","Update","Delete","Merge","CreateTable","CreateIndex","Alter","Drop","Truncate","CreateView");
        if(!allowed.contains(type))throw Protocol.unsupported();return sql;
    }
    static void change(Protocol.Input input,Profiles profile,Connection connection,JsonNode spec,Map<String,Object> receipt)throws Exception {
        Protocol.keys(spec,"sql","parameters","transaction","role","batch","isolation");
        String mode=Protocol.text(spec,"transaction",20);boolean transaction=mode.equals("required");
        if(!transaction&&!mode.equals("autocommit")||transaction&&!profile.transactions()||!Protocol.optional(spec,"role",128).isEmpty())throw Protocol.unsupported();
        List<JsonNode> statements=new ArrayList<>();
        if(spec.has("batch")){Protocol.keys(spec.path("batch"),"statements");JsonNode batch=spec.path("batch").path("statements");if(!batch.isArray()||batch.size()<1||batch.size()>100||!Protocol.optional(spec,"sql",65536).isEmpty()||spec.has("parameters")||input.stepDigests().size()!=batch.size())throw Protocol.invalid();batch.forEach(statements::add);}
        else statements.add(spec);
        for(JsonNode statement:statements)changeSQL(Protocol.text(statement,"sql",65536),transaction);
        String level=Protocol.optional(spec,"isolation",32);
        if(!level.isEmpty()){
            if(!transaction)throw Protocol.unsupported();int isolation=switch(level){case "read_committed"->Connection.TRANSACTION_READ_COMMITTED;case "repeatable_read"->Connection.TRANSACTION_REPEATABLE_READ;case "serializable"->Connection.TRANSACTION_SERIALIZABLE;default->throw Protocol.unsupported();};
            if(!connection.getMetaData().supportsTransactionIsolationLevel(isolation))throw Protocol.unsupported();connection.setTransactionIsolation(isolation);
        }
        if(transaction){if(!connection.getMetaData().supportsTransactions())throw Protocol.unsupported();connection.setAutoCommit(false);}
        failed(receipt,"SOURCE_FAILED");int completed=0,attempted=0;long total=0;boolean known=true,committing=false,rolledBack=false;
        try {
            for(JsonNode statement:statements){
                try(PreparedStatement query=connection.prepareStatement(Protocol.text(statement,"sql",65536))){
                    timeout(query,input);Parameters.bind(query,statement.path("parameters"));attempted++;
                    long count=query.executeLargeUpdate();completed++;
                    if(count<0)known=false;else if(known)try{total=Math.addExact(total,count);}catch(ArithmeticException e){known=false;}
                }
            }
            if(transaction){committing=true;connection.commit();}
            complete(receipt);receipt.put("effect","committed");if(known)receipt.put("affected_rows",total);
        }catch(Exception e){
            if(transaction&&!committing)try{connection.rollback();rolledBack=true;}catch(Exception ignored){}
            if(transaction&&rolledBack){completed=0;receipt.put("effect","none");}
            else if(attempted>completed||committing||transaction&&attempted>0){receipt.put("outcome","outcome_unknown");receipt.put("effect","unknown");receipt.put("error_code","OUTCOME_UNKNOWN");}
            else if(completed>0)receipt.put("effect","partial");
        }finally{
            if(spec.has("batch")){
                List<Map<String,Object>> steps=new ArrayList<>();boolean unknown=receipt.get("outcome").equals("outcome_unknown");
                for(int i=0;i<statements.size();i++){String effect="none";if(transaction){if(receipt.get("outcome").equals("completed"))effect="committed";else if(unknown&&i<attempted)effect="unknown";}else if(i<completed)effect="committed";else if(unknown&&i<attempted)effect="unknown";steps.add(Map.of("index",i,"sha256",input.stepDigests().get(i),"effect",effect));}
                receipt.put("steps",steps);
            }
        }
    }
}
