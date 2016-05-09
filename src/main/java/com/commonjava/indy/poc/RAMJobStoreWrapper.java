package com.commonjava.indy.poc;

import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.simpl.RAMJobStore;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

import java.util.List;

public class RAMJobStoreWrapper
        extends RAMJobStore
{

    @Override
    public void initialize( ClassLoadHelper loadHelper, SchedulerSignaler schedSignaler )
    {
        super.initialize( loadHelper, schedSignaler );
        System.out.println( "Wrapper: RAM store initialized!" );
    }

    @Override
    public void schedulerStarted()
    {
        super.schedulerStarted();
        System.out.println( "Wrapper: RAM store scheduler started!" );
    }

    @Override
    public void schedulerPaused()
    {
        super.schedulerPaused();
        System.out.println( "Wrapper: RAM store scheduler paused!" );
    }

    @Override
    public void schedulerResumed()
    {
        super.schedulerResumed();
        System.out.println( "Wrapper: RAM store scheduler resumed!" );
    }

    @Override
    public boolean removeJob( JobKey jobKey )
    {
        System.out.println( "Wrapper: RAM store scheduler remove job " + jobKey + "!" );
        return super.removeJob( jobKey );
    }

    @Override
    public boolean removeJobs( List<JobKey> jobKeys )
            throws JobPersistenceException
    {
        System.out.println( "Wrapper: RAM store scheduler remove jobs " + jobKeys + "!" );
        return super.removeJobs( jobKeys );
    }
}
