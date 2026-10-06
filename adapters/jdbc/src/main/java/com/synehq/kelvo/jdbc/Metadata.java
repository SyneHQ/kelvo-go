// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.*;
import java.util.*;

final class Metadata {
    record Selection(String object,String catalog,String schema,String name,long offset,int limit,Map<String,String> columns){}
    static Selection validate(Protocol.Input input,JsonNode spec){
        Protocol.keys(spec,"object","target","cursor","limit");JsonNode target=spec.path("target");Protocol.keys(target,"catalog","schema","name");
        String object=Protocol.text(spec,"object",40),catalog=Protocol.optional(target,"catalog",128),schema=Protocol.optional(target,"schema",128),name=Protocol.optional(target,"name",128),cursor=Protocol.optional(spec,"cursor",7);
        if(catalog.isEmpty())catalog=Protocol.text(input.source(),"database",128);
        if(!catalog.equals(Protocol.text(input.source(),"database",128)))throw Protocol.invalid();
        String bound=Protocol.optional(input.source(),"schema",128);if(schema.isEmpty())schema=bound;else if(!bound.isEmpty()&&!schema.equals(bound))throw Protocol.invalid();
        if(!Profiles.identifier(schema,true)||!Profiles.identifier(name,true))throw Protocol.invalid();
        long offset=cursor.isEmpty()?0:Long.parseLong(cursor);if(offset<0||offset>1_000_000||!cursor.isEmpty()&&!Long.toString(offset).equals(cursor))throw Protocol.invalid();
        long limit=Protocol.integer(spec,"limit");if(limit<1||limit>10000||limit>input.rows())throw Protocol.invalid();
        Map<String,String> columns=switch(object){
            case "catalogs","databases" -> Map.of("TABLE_CAT","catalog");
            case "schemas" -> Map.of("TABLE_CATALOG","catalog","TABLE_SCHEM","schema_name");
            case "tables" -> Map.of("TABLE_CAT","catalog","TABLE_SCHEM","schema_name","TABLE_NAME","name","TABLE_TYPE","type");
            case "columns" -> Map.of("TABLE_SCHEM","schema_name","TABLE_NAME","table_name","COLUMN_NAME","name","TYPE_NAME","type","ORDINAL_POSITION","position","IS_NULLABLE","nullable","COLUMN_DEF","default_value","COLUMN_SIZE","numeric_precision","DECIMAL_DIGITS","numeric_scale");
            case "primary_keys" -> Map.of("TABLE_SCHEM","schema_name","TABLE_NAME","table_name","PK_NAME","name","COLUMN_NAME","column_name","KEY_SEQ","position");
            case "foreign_keys","relationships" -> Map.of("FKTABLE_SCHEM","schema_name","FKTABLE_NAME","table_name","FK_NAME","name","FKCOLUMN_NAME","column_name","KEY_SEQ","position","PKTABLE_SCHEM","referenced_schema","PKTABLE_NAME","referenced_table","PKCOLUMN_NAME","referenced_column");
            case "indexes" -> Map.of("TABLE_SCHEM","schema_name","TABLE_NAME","table_name","INDEX_NAME","name","COLUMN_NAME","column_name","ORDINAL_POSITION","position","NON_UNIQUE","non_unique");
            case "functions" -> Map.of("FUNCTION_CAT","catalog","FUNCTION_SCHEM","schema_name","FUNCTION_NAME","name","REMARKS","description","SPECIFIC_NAME","specific_name");
            case "procedures" -> Map.of("PROCEDURE_CAT","catalog","PROCEDURE_SCHEM","schema_name","PROCEDURE_NAME","name","REMARKS","description","SPECIFIC_NAME","specific_name");
            default -> throw Protocol.unsupported();
        };
        if(Set.of("primary_keys","foreign_keys","relationships","indexes").contains(object)&&name.isEmpty())throw Protocol.invalid();
        return new Selection(object,catalog,schema,name,offset,(int)limit,columns);
    }
    static String pattern(DatabaseMetaData db,String name)throws SQLException{if(name.isEmpty())return "%";String escape=db.getSearchStringEscape();if(escape==null||escape.isEmpty()) {if(name.contains("_"))throw Protocol.unsupported();return name;}return name.replace(escape,escape+escape).replace("_",escape+"_").replace("%",escape+"%");}
    static ResultSet open(DatabaseMetaData db,Selection s,Profiles profile)throws SQLException{
        String catalog=s.catalog(),schema=s.schema();
        if(profile.engine.equals("h2")) {
            String actual=db.getConnection().getCatalog();
            if(actual==null||!actual.equalsIgnoreCase(catalog))throw Protocol.unsupported();
            catalog=actual;
        } else if(profile.engine.equals("hive")||profile.engine.equals("spark")) {
            catalog=null;if(schema.isEmpty())schema=profile.database;
        } else if(profile.engine.equals("db2")||profile.engine.equals("sap_hana"))catalog=null;
        return switch(s.object()){
            // Catalog enumeration is scoped to the saved database. A generic
            // driver may not support this filter, so return a local JDBC rowset.
            case "catalogs","databases" -> catalog(s.catalog());
            case "schemas" -> db.getSchemas(catalog,pattern(db,schema));
            case "tables" -> db.getTables(catalog,pattern(db,schema),pattern(db,s.name()),new String[]{"TABLE","VIEW"});
            case "columns" -> db.getColumns(catalog,pattern(db,schema),pattern(db,s.name()),"%");
            case "primary_keys" -> db.getPrimaryKeys(catalog,schema.isEmpty()?null:schema,s.name());
            case "foreign_keys","relationships" -> db.getImportedKeys(catalog,schema.isEmpty()?null:schema,s.name());
            case "indexes" -> db.getIndexInfo(catalog,schema.isEmpty()?null:schema,s.name(),false,false);
            case "functions" -> db.getFunctions(catalog,pattern(db,schema),pattern(db,s.name()));
            case "procedures" -> db.getProcedures(catalog,pattern(db,schema),pattern(db,s.name()));
            default -> throw Protocol.unsupported();
        };
    }
    static ResultSet catalog(String catalog)throws SQLException{
        var rows=javax.sql.rowset.RowSetProvider.newFactory().createCachedRowSet();var meta=new javax.sql.rowset.RowSetMetaDataImpl();meta.setColumnCount(1);meta.setColumnName(1,"TABLE_CAT");meta.setColumnLabel(1,"TABLE_CAT");meta.setColumnType(1,Types.VARCHAR);rows.setMetaData(meta);rows.moveToInsertRow();rows.updateString(1,catalog);rows.insertRow();rows.moveToCurrentRow();rows.beforeFirst();return rows;
    }
}
