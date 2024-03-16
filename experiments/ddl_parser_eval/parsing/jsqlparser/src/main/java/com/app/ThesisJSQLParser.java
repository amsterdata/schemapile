package com.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.TokenMgrException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.procedure.CreateProcedure;
import net.sf.jsqlparser.statement.create.schema.CreateSchema;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ForeignKeyIndex;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.table.NamedConstraint;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.validation.Validation;
import net.sf.jsqlparser.util.validation.ValidationError;
import net.sf.jsqlparser.util.validation.feature.DatabaseType;
import net.sf.jsqlparser.util.validation.feature.FeaturesAllowed;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.comment.Comment;

public class JSQLParserApp {
    private String sql;

    private String err_tried_without_brackets;
    private String err_tried_with_brackets;

    public JSQLParserApp() {
        sql = "";
        err_tried_without_brackets ="";
        err_tried_with_brackets = "";
    }

    public JSQLParserApp(String s) {
        sql = s;
        err_tried_without_brackets ="";
        err_tried_with_brackets = "";
    }

    private String get_str_list(ArrayList<String> arr) {
        String str_arr = new String("");
        for (int i = 0; i < arr.size(); i++) {
            str_arr += arr.get(i);
            str_arr += ";";
        }

        return str_arr;
    }

    public String get_err_tried_with_brackets() {
        return this.err_tried_with_brackets;
    }

    public String get_err_tried_without_brackets() {
        return this.err_tried_without_brackets;
    }

    public String parse() {
        
        final ArrayList<String> tblNames = new ArrayList<String>();
        final ArrayList<String> colDefNames = new ArrayList<String>();
        final ArrayList<String> viewNames = new ArrayList<String>();
        final ArrayList<String> schNames = new ArrayList<String>();
        final ArrayList<String> dbNames = new ArrayList<String>();

        final ArrayList<Integer> num_ctr_notnull = new ArrayList<Integer>();
        final ArrayList<Integer> num_ctr_unique = new ArrayList<Integer>();
        final ArrayList<Integer> num_ctr_primary = new ArrayList<Integer>();
        final ArrayList<Integer> num_ctr_foreign = new ArrayList<Integer>();

        StatementVisitorAdapter statementVisitor = new StatementVisitorAdapter() {

            public void visit(Select select) {
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList = tablesNamesFinder.getTableList(select);
                if (tableList != null) {
                    for (String tbl : tableList) {
                        tblNames.add(tbl);
                    }
                }
            }

            public void visit(CreateTable create) {
     
                tblNames.add(create.getTable().getName());
                schNames.add(create.getTable().getSchemaName());
                dbNames.add(create.getTable().getDatabase().getDatabaseName());

                List<ColumnDefinition> x = create.getColumnDefinitions();

                List<Index> pk_fk = create.getIndexes();
                try {
                    for (Index item : pk_fk) {
                        //System.out.println("IDX-" + item.toString());
                        if (item instanceof net.sf.jsqlparser.statement.create.table.ForeignKeyIndex) {
                            //System.out.println("FK INDEX: " + item.toString());
                            ForeignKeyIndex fkidx = (ForeignKeyIndex) item;
                            tblNames.add(fkidx.getTable().toString());
                        }

                        if (item.getType().toUpperCase().contains("PRIMARY")) {
                            
                            num_ctr_primary.add(1);
                        }
                        if (item.getType().toUpperCase().contains("FOREIGN")) {
                            //System.out.println("INDEX: " + item.toString());
                            num_ctr_foreign.add(1);
                        }
                        if (item.getType().toUpperCase().contains("UNIQUE")) {
                            num_ctr_unique.add(1);
                        }
                    }
                } catch (NullPointerException e) {
                    ;
                }
                
                if (x != null) {
                for (ColumnDefinition y : x) {
                    colDefNames.add(y.getColumnName());
                    
                    try {
                        
                        for (String z:y.getColumnSpecs()) {
                            
                            
                            if (z.toUpperCase().equals("UNIQUE")) {
                                num_ctr_unique.add(1);
                            }
                            if (z.toUpperCase().equals("NOT")) {
                                
                                num_ctr_notnull.add(1);
                            }
                            if (z.toUpperCase().equals("PRIMARY")) {
                                
                                num_ctr_primary.add(1);
                            }
                            if (z.toUpperCase().equals("REFERENCES")) {
                                num_ctr_foreign.add(1);
                            }
                            }
                            
                        }
                    catch (NullPointerException e) {
                        ;
                    }
                }   
            }
        }


            public void visit (Drop drop) {
                //if (!drop.isMaterialized()) {
                
                if (drop.getType().toUpperCase().equals("TABLE")) {
                    tblNames.add(drop.getName().getName());
                    schNames.add(drop.getName().getSchemaName());
                    dbNames.add(drop.getName().getDatabase().getDatabaseName());
                }
                
                if (drop.getType().toUpperCase().equals("VIEW")) {
                    viewNames.add(drop.getName().getName());
                    schNames.add(drop.getName().getSchemaName());
                    dbNames.add(drop.getName().getDatabase().getDatabaseName());
                }
            }


            public void visit (Delete delete) {
                tblNames.add(delete.getTable().getName());
                schNames.add(delete.getTable().getSchemaName());
                dbNames.add(delete.getTable().getDatabase().getDatabaseName());
                try {
                    for (Table x:delete.getTables()){
                        tblNames.add(x.getName());
                        schNames.add(x.getSchemaName());
                        dbNames.add(x.getDatabase().getDatabaseName());
                    }
                } catch (NullPointerException e) {
                    ;
                }
                
            }


            public void visit (Upsert replace) {
                tblNames.add(replace.getTable().getName());
                schNames.add(replace.getTable().getSchemaName());
                dbNames.add(replace.getTable().getDatabase().getDatabaseName());
            }


            public void visit (Update update) {
                tblNames.add(update.getTable().getName());
                schNames.add(update.getTable().getSchemaName());
                dbNames.add(update.getTable().getDatabase().getDatabaseName());
            }

            public void visit (Truncate truncate) {
                tblNames.add(truncate.getTable().getName());
                schNames.add(truncate.getTable().getSchemaName());
                dbNames.add(truncate.getTable().getDatabase().getDatabaseName());
            }
            
            public void visit (Merge merge) {
                tblNames.add(merge.getTable().getName());
                schNames.add(merge.getTable().getSchemaName());
                dbNames.add(merge.getTable().getDatabase().getDatabaseName());
            }

            public void visit (Insert insert) {
                tblNames.add(insert.getTable().getName());
                schNames.add(insert.getTable().getSchemaName());
                dbNames.add(insert.getTable().getDatabase().getDatabaseName());
                
            }

            public void visit (Grant grant) {
                
            }


            public void visit (Execute exc) {
                
            }

            public void visit (CreateView view) {
                viewNames.add(view.getView().getName());
                schNames.add(view.getView().getSchemaName());
                dbNames.add(view.getView().getDatabase().getDatabaseName());
            }

            public void visit (AlterView view) {
                viewNames.add(view.getView().getName());
                schNames.add(view.getView().getSchemaName());
                dbNames.add(view.getView().getDatabase().getDatabaseName());
            }

            public void visit (CreateSchema sch) {
                schNames.add(sch.getSchemaName());
            }

            public void visit (CreateIndex idx) {
                tblNames.add(idx.getTable().getName());
                schNames.add(idx.getTable().getSchemaName());
                dbNames.add(idx.getTable().getDatabase().getDatabaseName());
            }

            public void visit (Comment cmt) {
                try {
                    tblNames.add(cmt.getTable().getName());
                    schNames.add(cmt.getTable().getSchemaName());
                    dbNames.add(cmt.getTable().getDatabase().getDatabaseName());
                } catch (NullPointerException e ) {;}
            }

            public void visit (Alter alter) {
                
                
                tblNames.add(alter.getTable().getName());
                schNames.add(alter.getTable().getSchemaName());
                dbNames.add(alter.getTable().getDatabase().getDatabaseName());

                try {
                    for (AlterExpression x:alter.getAlterExpressions()) {
                        
                        Index pk_fk = x.getIndex();
                        try {
                            
                            if (pk_fk instanceof net.sf.jsqlparser.statement.create.table.ForeignKeyIndex) {
                               
                                ForeignKeyIndex fkidx = (ForeignKeyIndex) pk_fk;
                                tblNames.add(fkidx.getTable().toString());
                                
                            }
                            
                            if (pk_fk.getType().toUpperCase().contains("PRIMARY")) {
                                
                                num_ctr_primary.add(1);
                            }
                            if (pk_fk.getType().toUpperCase().contains("FOREIGN")) {
                                
                                num_ctr_foreign.add(1);
                            }

                            if (pk_fk.getType().toUpperCase().contains("UNIQUE")) {
                                
                                num_ctr_unique.add(1);
                            }
                        } catch (NullPointerException e) {
                            ;
                        }

                        String newtbl = x.getFkSourceTable();
                        if (newtbl != null) {
                            tblNames.add(newtbl);
                        }
                        newtbl = x.getFkSourceSchema();
                        if (newtbl != null) {
                            schNames.add(newtbl);
                        }

                        try {
                            for (String y:x.getFkColumns()){
                                
                                num_ctr_foreign.add(1);
                                
                            }
                        } catch (NullPointerException e) {}

                        try {
                            for (String y:x.getPkColumns()){
                                
                                num_ctr_primary.add(1);
                                
                            }
                        } catch (NullPointerException e) {}

                        try {
                            for (String y:x.getUkColumns()){
                                
                                num_ctr_unique.add(1);
                            
                            }
                        } catch (NullPointerException e) {;}

                        List<AlterExpression.ColumnDataType> newcols = x.getColDataTypeList();
                        try {
                            AlterOperation op = x.getOperation();
                            
                            for (AlterExpression.ColumnDataType nc : newcols) {
                                if (nc.toString().toUpperCase().contains("NOT NULL")) {
                                    num_ctr_notnull.add(1);
                                }
                                if (op == AlterOperation.ADD) {
                                    try {
                                        colDefNames.add(nc.toString().split(" ")[0]);
                                    } catch (Exception e) {;}
                                }
                            }
                        } catch (NullPointerException e) {;}
                        
                    }
                } catch (NullPointerException e) {;}
            }

        };

        int val_ansi = 0;
        FeaturesAllowed myAllowedFeatures = new FeaturesAllowed("myAllowedFeatures").add(
            FeaturesAllowed.DDL, FeaturesAllowed.DML,FeaturesAllowed.EXPRESSIONS,DatabaseType.ANSI_SQL);
        Validation validation = new Validation(Arrays.asList(myAllowedFeatures), sql);
        List<ValidationError> errors = validation.validate();
        if (errors.isEmpty()) {
            val_ansi = 1;
        }

        int val_oracle = 0;
        myAllowedFeatures = new FeaturesAllowed("myAllowedFeatures").add(
            FeaturesAllowed.DDL, FeaturesAllowed.DML,FeaturesAllowed.EXPRESSIONS,DatabaseType.ORACLE);
        validation = new Validation(Arrays.asList(myAllowedFeatures), sql);
        errors = validation.validate();
        if (errors.isEmpty()) {
            val_oracle = 1;
        }
        
        int val_postgres = 0;
        myAllowedFeatures = new FeaturesAllowed("myAllowedFeatures").add(
            FeaturesAllowed.DDL, FeaturesAllowed.DML,FeaturesAllowed.EXPRESSIONS,DatabaseType.POSTGRESQL);
        validation = new Validation(Arrays.asList(myAllowedFeatures), sql);
        errors = validation.validate();
        if (errors.isEmpty()) {
            val_postgres = 1;
        }

        int val_mysql = 0;
        myAllowedFeatures = new FeaturesAllowed("myAllowedFeatures").add(
            FeaturesAllowed.DDL, FeaturesAllowed.DML,FeaturesAllowed.EXPRESSIONS,DatabaseType.MYSQL);
        validation = new Validation(Arrays.asList(myAllowedFeatures), sql);
        errors = validation.validate();
        if (errors.isEmpty()) {
            val_mysql = 1;
        }

        int val_sqlserver = 0;
        myAllowedFeatures = new FeaturesAllowed("myAllowedFeatures").add(
            FeaturesAllowed.DDL, FeaturesAllowed.DML,FeaturesAllowed.EXPRESSIONS,DatabaseType.SQLSERVER);
        validation = new Validation(Arrays.asList(myAllowedFeatures), sql);
        errors = validation.validate();
        if (errors.isEmpty()) {
            val_sqlserver = 1;
        }

        int parsed_overall = 1;
        Statements stmts = null;
        String error_nobrackets = new String("");
        String error_brackets = new String("");

        try {
            stmts = CCJSqlParserUtil.parseStatements(new CCJSqlParser(sql)
            .withSquareBracketQuotation(false)
            //.withBackslashEscapeCharacter(true)
            //.withTimeOut(6000)
            );
        } catch (JSQLParserException | ParseException | TokenMgrException | Error e1) {
            error_nobrackets += e1.getMessage();
            try {
                stmts = CCJSqlParserUtil.parseStatements(new CCJSqlParser(sql)
                    .withSquareBracketQuotation(true)
                );
                
            } catch (JSQLParserException | ParseException | TokenMgrException | Error e2) {
                parsed_overall = 0;
                error_brackets += e2.getMessage();
            }
        }

        int sum_num_ctr_notnull = 0;
        int sum_num_ctr_unique = 0;
        int sum_num_ctr_primary = 0;
        int sum_num_ctr_foreign = 0;

        int num_statements = 0;
        if ((parsed_overall == 1 ) && (stmts != null)) {
            for (Statement stmt : stmts.getStatements()) {
                num_statements += 1;
                //System.out.println("=============");
                //System.out.println(stmt.toString());
                //System.out.println("=============");
                stmt.accept(statementVisitor);
            }
                schNames.removeAll(Collections.singleton(null));
                dbNames.removeAll(Collections.singleton(null));
                viewNames.removeAll(Collections.singleton(null));
                colDefNames.removeAll(Collections.singleton(null));
                tblNames.removeAll(Collections.singleton(null));

                for (int i = 0; i < tblNames.size(); i++) {
                    tblNames.set(i, tblNames.get(i).replace("[","").replace("]","").replace("'","").replace("`","").replace("\"",""));
                }

                for (int i = 0; i < colDefNames.size(); i++) {
                    colDefNames.set(i, colDefNames.get(i).replace("[","").replace("]","").replace("'","").replace("`","").replace("\"",""));
                }

                for (int i = 0; i < viewNames.size(); i++) {
                    viewNames.set(i, viewNames.get(i).replace("[","").replace("]","").replace("'","").replace("`","").replace("\"",""));
                }

                for (int i = 0; i < schNames.size(); i++) {
                    schNames.set(i, schNames.get(i).replace("[","").replace("]","").replace("'","").replace("`","").replace("\"",""));
                }

                for (int i = 0; i < dbNames.size(); i++) {
                    dbNames.set(i, dbNames.get(i).replace("[","").replace("]","").replace("'","").replace("`","").replace("\"",""));
                }

                for (Integer i:num_ctr_notnull) {
                    sum_num_ctr_notnull += i.intValue();
                }
                
                for (Integer i:num_ctr_unique) {
                    sum_num_ctr_unique += i.intValue();
                }
                
                for (Integer i:num_ctr_primary) {
                    sum_num_ctr_primary += i.intValue();
                }
                
                for (Integer i:num_ctr_foreign) {
                    sum_num_ctr_foreign += i.intValue();
                }
        
        } else {
            ;
        }

        String ret_value = new String("");

        String str_tables = this.get_str_list(tblNames);
        String str_cols = this.get_str_list(colDefNames);
        String str_dbs = this.get_str_list(dbNames);
        String str_schs = this.get_str_list(schNames);
        String str_views = this.get_str_list(viewNames);

        /* format: SUCCESS#blabla or ERROR#blabla
         * format: parsed_overall|num_stmts|valid_ansi|valid_oracle|valid_mysql|valid_postgres|valid_sqlserver|[tables]|[cols]|[schs]|[dbs]|[views]|ctr_nn|ctr_uq|ctr_pk|ctr_fk
         * ";|;" instead of "|"
         */

        err_tried_with_brackets = error_brackets;
        err_tried_without_brackets = error_nobrackets;

        ret_value += Integer.toString(parsed_overall) + ";|;" 
        //+ error_nobrackets + ";|;"
        //+ error_brackets + ";|;"
        + Integer.toString(num_statements) + ";|;"
        + Integer.toString(val_ansi) + ";|;"
        + Integer.toString(val_oracle) + ";|;"
        + Integer.toString(val_mysql) + ";|;"
        + Integer.toString(val_postgres) + ";|;"
        + Integer.toString(val_sqlserver) + ";|;"
        + str_tables + ";|;" + str_cols + ";|;" + str_schs + ";|;" + str_dbs + ";|;" + str_views + ";|;"
        + Integer.toString(sum_num_ctr_notnull) + ";|;"
        + Integer.toString(sum_num_ctr_unique) + ";|;"
        + Integer.toString(sum_num_ctr_primary) + ";|;"
        + Integer.toString(sum_num_ctr_foreign) + ";|;"
        ;

        return ret_value;
    }
}