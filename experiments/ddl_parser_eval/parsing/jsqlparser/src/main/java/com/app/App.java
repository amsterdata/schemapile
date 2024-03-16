package com.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.app.JSQLParserApp;

import py4j.GatewayServer;

public class App 
{
    private String sql;
    private JSQLParserApp p;

    public App() {
        // entrypoint
        sql = new String("");
    }

    public JSQLParserApp getParserObj(String s) {
        this.sql = s;
        p = new JSQLParserApp(this.sql);
        return p;
    }

    public static void main( String[] args )
    {   
        GatewayServer gatewayServer = new GatewayServer(new App());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }       
}

