// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.*;
import java.sql.*;
import java.time.*;
import java.util.*;

final class Parameters {
    static void bind(PreparedStatement statement, JsonNode parameters) throws Exception {
        if(parameters.isMissingNode())return;
        if(!parameters.isArray()||parameters.size()>1000)throw Protocol.invalid();
        int index=1;
        for(JsonNode parameter:parameters) {
            Protocol.keys(parameter,"type","value");String type=Protocol.text(parameter,"type",20);JsonNode value=parameter.path("value");
            String lexical=value.isTextual()?value.textValue():value.toString();
            switch(type){
                case "null" -> {if(!value.isNull())throw Protocol.invalid();statement.setNull(index,Types.NULL);}
                case "bool" -> {if(!value.isBoolean())throw Protocol.invalid();statement.setBoolean(index,value.booleanValue());}
                case "int8","int16","int32","int64","uint8","uint16","uint32","uint64" -> {
                    boolean unsigned=type.startsWith("u");int width=Integer.parseInt(type.substring(unsigned?4:3));
                    if(!lexical.matches("-?(0|[1-9][0-9]*)"))throw Protocol.invalid();
                    BigInteger number=new BigInteger(lexical);BigInteger low=unsigned?BigInteger.ZERO:BigInteger.ONE.shiftLeft(width-1).negate(), high=BigInteger.ONE.shiftLeft(unsigned?width:width-1).subtract(BigInteger.ONE);
                    if(number.compareTo(low)<0||number.compareTo(high)>0)throw Protocol.invalid();
                    if(unsigned&&width==64)statement.setBigDecimal(index,new BigDecimal(number));else statement.setLong(index,number.longValueExact());
                }
                case "decimal128","decimal256" -> {BigDecimal decimal=new BigDecimal(lexical);if(decimal.precision()>(type.equals("decimal128")?38:76))throw Protocol.invalid();statement.setBigDecimal(index,decimal);}
                case "float32" -> {float number=Float.parseFloat(lexical);if(!Float.isFinite(number))throw Protocol.invalid();statement.setFloat(index,number);}
                case "float64" -> {double number=Double.parseDouble(lexical);if(!Double.isFinite(number))throw Protocol.invalid();statement.setDouble(index,number);}
                case "string" -> {if(!value.isTextual())throw Protocol.invalid();statement.setString(index,value.textValue());}
                case "binary" -> {if(!value.isTextual())throw Protocol.invalid();byte[] binary=Base64.getDecoder().decode(lexical);if(!Base64.getEncoder().encodeToString(binary).equals(lexical))throw Protocol.invalid();statement.setBytes(index,binary);}
                case "date" -> statement.setObject(index,LocalDate.parse(lexical),Types.DATE);
                case "timestamp" -> statement.setObject(index,OffsetDateTime.parse(lexical),Types.TIMESTAMP_WITH_TIMEZONE);
                case "json" -> statement.setString(index,Protocol.JSON.writeValueAsString(value));
                default -> throw Protocol.unsupported();
            }
            index++;
        }
    }
}
