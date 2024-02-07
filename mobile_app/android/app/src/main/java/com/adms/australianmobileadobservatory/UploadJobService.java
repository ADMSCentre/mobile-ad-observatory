/*
*
* This class deals with scheduling the job
*
* */

package com.adms.australianmobileadobservatory;

import android.app.job.JobInfo;
import android.app.job.JobParameters;
import android.app.job.JobScheduler;
import android.app.job.JobService;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;

public class UploadJobService extends JobService {
    private static final String TAG = "UploadJobService";
    private static final int jobServiceID = Settings.jobServiceID;

    /*
    *
    * This method is executed when starting the JobService
    *
    * */
    @Override
    public boolean onStartJob(JobParameters params) {
        Intent work = new Intent(this, EventDetectionService.class);
        EventDetectionService.enqueueWork(this, EventDetectionService.class, jobServiceID, work);
        return true;
    }

    /*
    *
    * This method is executed when stopping the JobService
    *
    * */
    @Override
    public boolean onStopJob(JobParameters params) {
        return false;
    }

    /*
    *
    * This method is executed to schedule the JobService
    *
    * */
    public static void scheduleJob(Context context) {
        // If the service is not currently on
        if (!isJobServiceOn(context)) {
            // Set up the job and schedule it to run when the device is charging, idle, and
            // on any network configuration
            ComponentName serviceComponent = new ComponentName(context, UploadJobService.class);
            JobInfo.Builder builder = new JobInfo.Builder(jobServiceID, serviceComponent);
            builder.setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY);
            builder.setRequiresDeviceIdle(false);
            builder.setRequiresCharging(true);
            JobScheduler jobScheduler = context.getSystemService(JobScheduler.class);
            jobScheduler.schedule(builder.build());
        }
    }

    /*
    *
    * This method determines whether the JobService is currently on
    *
    * */
    public static boolean isJobServiceOn( Context context ) {
        JobScheduler scheduler =
              (JobScheduler) context.getSystemService(Context.JOB_SCHEDULER_SERVICE);
        // Loop through all pending jobs
        for (JobInfo jobInfo : scheduler.getAllPendingJobs()) {
            // If this job's ID is found
            if (jobInfo.getId() == jobServiceID) {
                // Then yes, it has been scheduled
                return true;
            }
        }
        // Otherwise, default to indicating that it hasn't been scheduled
        return false;
    }

}