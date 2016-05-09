package com.commmonjava.indy.poc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.quartz.DateBuilder.nextGivenSecondDate;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class TestQuartz
{
    private static SchedulerFactory factory;

    static final String BING_FILE = "/tmp/bing.html";

    @BeforeClass
    public static void setUpEnv()
            throws Exception
    {
        final InputStream stream =
                TestQuartz.class.getClassLoader().getResourceAsStream( "META-INF/quartz.properties" );
        final Properties props = new Properties();
        props.load( stream );
        factory = new StdSchedulerFactory( props );
        //        factory = new StdSchedulerFactory();
    }

    @Test
    public void test()
            throws Exception
    {
        final Scheduler sched = factory.getScheduler();
        final JobDetail job = newJob( HelloJob.class ).withIdentity( "job1", "group1" ).build();

        final Date runTime = nextGivenSecondDate( new Date(), 10 );

        // Trigger the job to run on the next round minute
        final Trigger trigger = newTrigger().withIdentity( "trigger1", "group1" ).startAt( runTime ).build();

        sched.scheduleJob( job, trigger );
        sched.start();

        Thread.sleep( 15L * 1000L );

        final File file = new File( BING_FILE );

        assertThat( "bing.html should exists!", file.exists(), equalTo( true ) );

        sched.shutdown( true );

    }

    @AfterClass
    public static void tearDownEnv()
            throws IOException
    {
        final File file = new File( BING_FILE );
        if ( file.exists() )
        {
            file.delete();
        }
    }
}
