/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.mombergm.EMRSME.exercise3;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.regex.RegexFilter;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.operation.text.FieldFormatter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AverageBy;
import cascading.pipe.assembly.Unique;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;


public class Main {
	
    public static void main(String[] args) {
 
    	// First, lets specify the Inputs and the Outputs.
    	// Basic Java Here to read the first and second argument passed to the JAR.
    	// http://docs.oracle.com/javase/tutorial/essential/environment/cmdLineArgs.html
        String inputPath = args[0];
        String outputPath = args[1];
        
        // This is where the Cascading starts. 
        // Sources and Sinks. The basic principle is quite simple, but took me a while to understand how the Schemas can be used to better associate the data. 
        // So, before we declare the Taps, lets Declare the Schema...
        Scheme inScheme = new TextDelimited( new Fields("LOAN_IDENT", "MONTHLY_PERIOD", "SERV_NAME", "CUR_INT_RATE", "CUR_UNPAID_PRINCIPAL_BAL","LOAN_AGE","REMAINING_MONTHS","ADJUSTING_REM","MAT_DATE","METRO_STAT","CUR LOAN","MOD_FLAG","ZOERO","ZBAL","REPIND"), "|");
        // Output must be displayed as FullMonthName $Amount.2decimals
        // So, here I split my fields with a space " " 
        Scheme outScheme = new TextDelimited( new Fields("CALENDAR_MONTH","FINAL_AVG_OUT"), " " ) ;
        
        //Okay, now, using the new Scheme, Lets set the Taps. 
        Tap inTap = new Hfs( inScheme, inputPath );
        Tap outTap = new Hfs( outScheme, outputPath );
        
        // At this point, I have the fields, but It's gonna bite me, cause I need the correct date format. 
        // A little digging into the Cascading docs though, shows that there is two Text Functions that can convert Dates.
        // Note to self, become a documentation Contributer to Cascading.. Their docs need to be improved. 
        // http://docs.cascading.org/cascading/2.6/userguide/html/ch09s05.html
        DateParser dateParser = new DateParser( new Fields( "EPOCH" ), "MM/dd/yyyy" );
       
        //Here I iterate through each line in the input using the first Pipe in the Pipe Assembly.
        // This pipe will convert the Date from MM/dd/yyy to EPOCH
        Pipe dateConverter = new Each("myDateConverter",new Fields("MONTHLY_PERIOD"), dateParser, Fields.ALL);   
        
        //Now I need to transform this over to A Month Name from EPOCH, so that I can Group My Entried per Month
        // First lets set the formatter.
        DateFormatter epochMonth = new DateFormatter( new Fields( "CALENDAR_MONTH" ), "MMMM" );
        dateConverter = new Each( dateConverter, new Fields( "EPOCH" ), epochMonth, Fields.ALL );
       
        // After my Hive Queries were run I couldn't understand why results were different...
        // So I went and I looked for the differences, I ran some queries with COUNT and I saw different results. 
        // Basically, Hive doesn't count null values, so average will be divided by different value. Cascading on the other hand does count null values. 
        // I had to get rid of these null Tuples, so I looked for a "ifNull" type function but couldn't find one.. There might still be one though. Need to investigate. 
        // The second best option was the RegexFilter, which filters a tuple and Keeps matches, unless removeMatch is set to true in witch case the tuple is removed.
        // http://docs.cascading.org/cascading/2.5/cascading-core/cascading/operation/regex/RegexFilter.html
        RegexFilter nullfilter = new RegexFilter( "^$" ,true);
        dateConverter = new Each( dateConverter, new Fields( "CUR_UNPAID_PRINCIPAL_BAL" ), nullfilter );
        
        // Okay, The output looks good, I have the Calendar month in Full Month Value..
        // Now I need to look for duplicates, Cascading doesn't have DISTINCT() like Hive/MYSQL but google seems to think that
        // Unique() works the same...
        // http://docs.cascading.org/cascading/2.6/userguide/html/ch10s06.html
        // Ahh, okay. So Unique requires you to specify the WHOLE row. Only specifying the Field you want unique does not bring over the other fields. 
        // I want to investigate this though... I might be wrong.
        dateConverter = new Unique( dateConverter, new Fields("LOAN_IDENT", "CALENDAR_MONTH", "SERV_NAME", "CUR_INT_RATE", "CUR_UNPAID_PRINCIPAL_BAL","LOAN_AGE","REMAINING_MONTHS","ADJUSTING_REM","MAT_DATE","METRO_STAT","CUR LOAN","MOD_FLAG","ZOERO","ZBAL","REPIND") );
      
        // Okay, I now have Unique Rows... Probably. Hahaha 
        // Now, lets get the Average of every month... Hive and MYSQL have avg(). Cascading has AverageBy()
        // Cascading also has Average(), but then I need to do an Every and run Average on each Tuple.
        // AverageBy seemed simpler.
        // http://docs.cascading.org/cascading/2.6/userguide/html/ch10.html#N21C4F
        dateConverter = new AverageBy( dateConverter, new Fields("CALENDAR_MONTH"), new Fields("CUR_UNPAID_PRINCIPAL_BAL"), new Fields("MONTHLY_AVERAGE") );
      
        // Booya! Okay, I have the Unique Fields with the average of those fields. Now I need to make them pretty.
        // This will probarbly be some Text Function in Cascading.. Oogling the docs...
        // Yaaarp. But damn was it hidden away, I almost didn't notice it at the bottom of the page without an example. 
        // FieldFormatter and it uses java.util.Formatter
        // http://docs.cascading.org/cascading/2.6/userguide/html/ch09s05.html
        // http://docs.oracle.com/javase/tutorial/java/data/numberformat.html
        FieldFormatter prettyFy = new FieldFormatter(new Fields("FINAL_AVG_OUT"), " $%.2f");
        dateConverter = new Each( dateConverter, new Fields("MONTHLY_AVERAGE"), prettyFy, Fields.ALL);
        
        FlowDef flowDef = FlowDef.flowDef()
    		   .addSource(dateConverter, inTap)
    		   .addTailSink(dateConverter, outTap)
    		   .setName("DataProcessing");
        Properties properties = AppProps.appProps()
        		.setName("DataProcessing")
        		.buildProperties();
        Flow parsedLogFlow = new Hadoop2MR1FlowConnector(properties).connect(flowDef);
        //Finally, execute the flow.
        parsedLogFlow.complete();
    }
}
