package com.commonjava.indy.poc;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class InfinispanJobStore
        implements JobStore
{
    protected DefaultCacheManager cacheManager = new DefaultCacheManager();

    protected Cache<JobKey, JobWrapper> jobsByKey;

    protected Cache<TriggerKey, TriggerWrapper> triggersByKey;

    protected Cache<String, Cache<JobKey, JobWrapper>> jobsByGroup;

    protected Cache<String, Cache<TriggerKey, TriggerWrapper>> triggersByGroup;

    protected Cache<String, Calendar> calendarsByName;

    protected TreeSet<TriggerWrapper> timeTriggers = new TreeSet<TriggerWrapper>( new TriggerWrapperComparator() );

    protected ArrayList<TriggerWrapper> triggers = new ArrayList<TriggerWrapper>( 1000 );

    protected final Object lock = new Object();

    protected HashSet<String> pausedTriggerGroups = new HashSet<String>();

    protected HashSet<String> pausedJobGroups = new HashSet<String>();

    protected HashSet<JobKey> blockedJobs = new HashSet<JobKey>();

    protected long misfireThreshold = 5000l;

    protected SchedulerSignaler signaler;

    private final Logger log = LoggerFactory.getLogger( getClass() );

    @Override
    public void initialize( ClassLoadHelper loadHelper, SchedulerSignaler signaler )
            throws SchedulerConfigException
    {
        jobsByKey = cacheManager.<JobKey, JobWrapper>getCache( "jobsByKey" );
        triggersByKey = cacheManager.<TriggerKey, TriggerWrapper>getCache( "triggersByKey" );
        jobsByGroup = cacheManager.<String, Cache<JobKey, JobWrapper>>getCache( "jobsByGroup" );
        triggersByGroup = cacheManager.<String, Cache<TriggerKey, TriggerWrapper>>getCache( "triggersByGroup" );
        calendarsByName = cacheManager.<String, Calendar>getCache( "calendarsByName" );

        this.signaler = signaler;

        log.info( "InfinispanJobStore initialized." );
    }

    @Override
    public void schedulerStarted()
            throws SchedulerException
    {

    }

    @Override
    public void schedulerPaused()
    {

    }

    @Override
    public void schedulerResumed()
    {

    }

    @Override
    public void shutdown()
    {
        cacheManager.stop();
    }

    @Override
    public boolean supportsPersistence()
    {
        return false;
    }

    /**
     * Clear (delete!) all scheduling data - all {@link org.quartz.Job}s, {@link Trigger}s
     * {@link Calendar}s.
     *
     * @throws JobPersistenceException
     */
    public void clearAllSchedulingData()
            throws JobPersistenceException
    {

        synchronized ( lock )
        {
            // unschedule jobs (delete triggers)
            List<String> lst = getTriggerGroupNames();
            for ( String group : lst )
            {
                Set<TriggerKey> keys = getTriggerKeys( GroupMatcher.triggerGroupEquals( group ) );
                for ( TriggerKey key : keys )
                {
                    removeTrigger( key );
                }
            }
            // delete jobs
            lst = getJobGroupNames();
            for ( String group : lst )
            {
                Set<JobKey> keys = getJobKeys( GroupMatcher.jobGroupEquals( group ) );
                for ( JobKey key : keys )
                {
                    removeJob( key );
                }
            }
            // delete calendars
            lst = getCalendarNames();
            for ( String name : lst )
            {
                removeCalendar( name );
            }
        }
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger()
    {
        return 0;
    }

    @Override
    public boolean isClustered()
    {
        return false;
    }

    @Override
    public void storeJobAndTrigger( JobDetail newJob, OperableTrigger newTrigger )
            throws ObjectAlreadyExistsException, JobPersistenceException
    {

    }

    @Override
    public void storeJob( JobDetail newJob, boolean replaceExisting )
            throws ObjectAlreadyExistsException, JobPersistenceException
    {

        JobWrapper jw = new JobWrapper( (JobDetail) newJob.clone() );

        boolean repl = false;

        if ( jobsByKey.get( jw.key ) != null )
        {
            if ( !replaceExisting )
            {
                throw new ObjectAlreadyExistsException( newJob );
            }
            repl = true;
        }

        if ( !repl )
        {
            // get job group
            Cache<JobKey, JobWrapper> grpMap = jobsByGroup.get( newJob.getKey().getGroup() );
            if ( grpMap == null )
            {
                grpMap = cacheManager.<JobKey, JobWrapper>getCache( "newJobsByKey" );
                jobsByGroup.put( newJob.getKey().getGroup(), grpMap );
            }
            // add to jobs by group
            grpMap.put( newJob.getKey(), jw );
            // add to jobs by FQN map
            jobsByKey.put( jw.key, jw );
        }
        else
        {
            // update job detail
            JobWrapper orig = jobsByKey.get( jw.key );
            orig.jobDetail = jw.jobDetail; // already cloned
        }

    }

    @Override
    public void storeJobsAndTriggers( Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace )
            throws ObjectAlreadyExistsException, JobPersistenceException
    {

    }

    @Override
    public boolean removeJob( JobKey jobKey )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public boolean removeJobs( List<JobKey> jobKeys )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public JobDetail retrieveJob( JobKey jobKey )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void storeTrigger( OperableTrigger newTrigger, boolean replaceExisting )
            throws ObjectAlreadyExistsException, JobPersistenceException
    {

    }

    @Override
    public boolean removeTrigger( TriggerKey triggerKey )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public boolean removeTriggers( List<TriggerKey> triggerKeys )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public boolean replaceTrigger( TriggerKey triggerKey, OperableTrigger newTrigger )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public OperableTrigger retrieveTrigger( TriggerKey triggerKey )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public boolean checkExists( JobKey jobKey )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public boolean checkExists( TriggerKey triggerKey )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public void storeCalendar( String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers )
            throws ObjectAlreadyExistsException, JobPersistenceException
    {

    }

    @Override
    public boolean removeCalendar( String calName )
            throws JobPersistenceException
    {
        return false;
    }

    @Override
    public Calendar retrieveCalendar( String calName )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public int getNumberOfJobs()
            throws JobPersistenceException
    {
        return 0;
    }

    @Override
    public int getNumberOfTriggers()
            throws JobPersistenceException
    {
        return 0;
    }

    @Override
    public int getNumberOfCalendars()
            throws JobPersistenceException
    {
        return 0;
    }

    @Override
    public Set<JobKey> getJobKeys( GroupMatcher<JobKey> matcher )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public Set<TriggerKey> getTriggerKeys( GroupMatcher<TriggerKey> matcher )
            throws JobPersistenceException
    {
        Set<TriggerKey> outList = null;

        StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
        String compareToValue = matcher.getCompareToValue();

        switch ( operator )
        {
            case EQUALS:
                Cache<TriggerKey, TriggerWrapper> grpMap = triggersByGroup.get( compareToValue );
                if ( grpMap != null )
                {
                    outList = new HashSet<TriggerKey>();

                    for ( TriggerWrapper tw : grpMap.values() )
                    {

                        if ( tw != null )
                        {
                            outList.add( tw.trigger.getKey() );
                        }
                    }
                }
                break;

            default:
                for ( Map.Entry<String, Cache<TriggerKey, TriggerWrapper>> entry : triggersByGroup.entrySet() )
                {
                    if ( operator.evaluate( entry.getKey(), compareToValue ) && entry.getValue() != null )
                    {
                        if ( outList == null )
                        {
                            outList = new HashSet<TriggerKey>();
                        }
                        for ( TriggerWrapper triggerWrapper : entry.getValue().values() )
                        {
                            if ( triggerWrapper != null )
                            {
                                outList.add( triggerWrapper.trigger.getKey() );
                            }
                        }
                    }
                }
        }

        return outList == null ? Collections.<TriggerKey>emptySet() : outList;
    }

    @Override
    public List<String> getJobGroupNames()
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public List<String> getTriggerGroupNames()
            throws JobPersistenceException
    {
        synchronized ( lock )
        {
            return new LinkedList<String>( triggersByGroup.keySet() );
        }
    }

    @Override
    public List<String> getCalendarNames()
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public List<OperableTrigger> getTriggersForJob( JobKey jobKey )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public Trigger.TriggerState getTriggerState( TriggerKey triggerKey )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void pauseTrigger( TriggerKey triggerKey )
            throws JobPersistenceException
    {

    }

    @Override
    public Collection<String> pauseTriggers( GroupMatcher<TriggerKey> matcher )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void pauseJob( JobKey jobKey )
            throws JobPersistenceException
    {

    }

    @Override
    public Collection<String> pauseJobs( GroupMatcher<JobKey> groupMatcher )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void resumeTrigger( TriggerKey triggerKey )
            throws JobPersistenceException
    {

    }

    @Override
    public Collection<String> resumeTriggers( GroupMatcher<TriggerKey> matcher )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public Set<String> getPausedTriggerGroups()
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void resumeJob( JobKey jobKey )
            throws JobPersistenceException
    {

    }

    @Override
    public Collection<String> resumeJobs( GroupMatcher<JobKey> matcher )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void pauseAll()
            throws JobPersistenceException
    {

    }

    @Override
    public void resumeAll()
            throws JobPersistenceException
    {

    }

    @Override
    public List<OperableTrigger> acquireNextTriggers( long noLaterThan, int maxCount, long timeWindow )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void releaseAcquiredTrigger( OperableTrigger trigger )
    {

    }

    @Override
    public List<TriggerFiredResult> triggersFired( List<OperableTrigger> triggers )
            throws JobPersistenceException
    {
        return null;
    }

    @Override
    public void triggeredJobComplete( OperableTrigger trigger, JobDetail jobDetail,
                                      Trigger.CompletedExecutionInstruction triggerInstCode )
    {

    }

    @Override
    public void setInstanceId( String schedInstId )
    {

    }

    @Override
    public void setInstanceName( String schedName )
    {

    }

    @Override
    public void setThreadPoolSize( int poolSize )
    {

    }
}

/*******************************************************************************
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 *
 * Helper Classes. * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 */

class TriggerWrapperComparator
        implements Comparator<TriggerWrapper>, java.io.Serializable
{

    private static final long serialVersionUID = 8809557142191514261L;

    Trigger.TriggerTimeComparator ttc = new Trigger.TriggerTimeComparator();

    public int compare( TriggerWrapper trig1, TriggerWrapper trig2 )
    {
        return ttc.compare( trig1.trigger, trig2.trigger );
    }

    @Override
    public boolean equals( Object obj )
    {
        return ( obj instanceof TriggerWrapperComparator );
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
}

class JobWrapper
{

    public JobKey key;

    public JobDetail jobDetail;

    JobWrapper( JobDetail jobDetail )
    {
        this.jobDetail = jobDetail;
        key = jobDetail.getKey();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof JobWrapper )
        {
            JobWrapper jw = (JobWrapper) obj;
            if ( jw.key.equals( this.key ) )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        return key.hashCode();
    }
}

class TriggerWrapper
{

    public final TriggerKey key;

    public final JobKey jobKey;

    public final OperableTrigger trigger;

    public int state = STATE_WAITING;

    public static final int STATE_WAITING = 0;

    public static final int STATE_ACQUIRED = 1;

    @SuppressWarnings( "UnusedDeclaration" )
    public static final int STATE_EXECUTING = 2;

    public static final int STATE_COMPLETE = 3;

    public static final int STATE_PAUSED = 4;

    public static final int STATE_BLOCKED = 5;

    public static final int STATE_PAUSED_BLOCKED = 6;

    public static final int STATE_ERROR = 7;

    TriggerWrapper( OperableTrigger trigger )
    {
        if ( trigger == null )
        {
            throw new IllegalArgumentException( "Trigger cannot be null!" );
        }
        this.trigger = trigger;
        key = trigger.getKey();
        this.jobKey = trigger.getJobKey();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof TriggerWrapper )
        {
            TriggerWrapper tw = (TriggerWrapper) obj;
            if ( tw.key.equals( this.key ) )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        return key.hashCode();
    }

    public OperableTrigger getTrigger()
    {
        return this.trigger;
    }
}