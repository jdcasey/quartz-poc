package com.commmonjava.indy.poc;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;

public class HelloJob
        implements Job
{

    public HelloJob()
    {
    }

    @Override
    public void execute( JobExecutionContext context )
            throws JobExecutionException
    {
        // Say Hello to the World and display the date/time
        System.out.println( "Hello World! - " + new Date() );
        try
        {
            writeFile();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    static void writeFile()
            throws IOException
    {
        final URLConnection connection = new URL( "https://www.bing.com/" ).openConnection();
        final InputStream inputStream = connection.getInputStream();
        byte[] buf = new byte[10000];
        inputStream.read( buf );
        final FileWriter writer = new FileWriter( new File( TestQuartz.BING_FILE ) );
        writer.write( new String( buf ) );
        inputStream.close();
        writer.close();
    }
}