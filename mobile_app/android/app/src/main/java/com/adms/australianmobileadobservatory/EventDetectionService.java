/*
*
* This class handles the Android formality of spinning up the LogManager with a JobIntentService.
*
* */

package com.adms.australianmobileadobservatory;

import android.content.Intent;
import android.util.Log;
import androidx.annotation.NonNull;
import androidx.core.app.JobIntentService;

public class EventDetectionService extends JobIntentService {
    public EventDetectionService(){super();}

    /*
    *
    * This method attempts to spin up the LogManager, as part of the intended functionality of the
    * EventDetectionService
    *
    * */
    @Override
    protected void onHandleWork(@NonNull Intent intent) {
        String TAG = "EventDetectionService";
        try {
            // Attempt to spin up the LogManager
            Log.i(TAG, "LogManager has been called from EventDetectionService");
            LogManager lManager = new LogManager(getApplicationContext());
            lManager.run();
        } catch (Exception e) {
            Log.e(TAG, "EventDetectionService has met a fatal error: ", e);
            throw new RuntimeException(e);
        }
    }
}
