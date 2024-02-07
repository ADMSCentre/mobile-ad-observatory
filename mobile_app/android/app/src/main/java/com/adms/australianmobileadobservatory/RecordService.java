/*
*
* This class deals with the recording service, responsible for managing the creation of
* screen recording video files
*
* */

package com.adms.australianmobileadobservatory;

import static android.app.Activity.RESULT_OK;
import static android.hardware.display.DisplayManager.VIRTUAL_DISPLAY_FLAG_PRESENTATION;

import static com.adms.australianmobileadobservatory.NotifyInactiveReceiver.constructNotification;
import static com.adms.australianmobileadobservatory.NotifyInactiveReceiver.constructNotificationForward;
import static com.adms.australianmobileadobservatory.NotifyInactiveReceiver.generateNotificationChannel;
import static com.adms.australianmobileadobservatory.NotifyInactiveReceiver.sendNotification;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_RECORDING_CHANNEL_DESCRIPTION;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_RECORDING_CHANNEL_ID;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_RECORDING_CHANNEL_ID_NAME;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_RECORDING_DESCRIPTION;
import static com.adms.australianmobileadobservatory.Settings.get_NOTIFICATION_RECORDING_TITLE;
import android.app.Notification;
import android.app.PendingIntent;
import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.hardware.display.VirtualDisplay;
import android.media.MediaRecorder;
import android.media.projection.MediaProjection;
import android.media.projection.MediaProjectionManager;
import android.os.BatteryManager;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.Looper;
import android.os.Message;
import android.os.Process;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.Surface;
import android.view.WindowManager;
import androidx.core.app.NotificationCompat;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public final class RecordService extends Service {
    private Intent data;
    private ServiceHandler mServiceHandler;
    private MediaProjection mMediaProjection;
    private VirtualDisplay mVirtualDisplay;
    private MediaRecorder mMediaRecorder;
    private BroadcastReceiver mScreenStateReceiver;
    private int resultCode;
    private static final String TAG = "RecordService";
    // The extra result code associated with the intent of the recording service
    private static final String EXTRA_RESULT_CODE = Settings.RECORD_SERVICE_EXTRA_RESULT_CODE;
    private static final String EXTRA_DATA = Settings.RECORD_SERVICE_EXTRA_DATA;
    // The ID of the notification associated with the recording service
    // (the value has no actual bearing on the functionality, although don't set it to zero:
    // https://developer.android.com/guide/components/foreground-services#:~:text=startForeground(ONGOING_NOTIFICATION_ID%2C%20notification)%3B)
    private static final int ONGOING_NOTIFICATION_ID = Settings.RECORD_SERVICE_ONGOING_NOTIFICATION_ID;
    // Whether or not the device screen is off
    public static boolean screenOff = false;
    // Whether or not a recording is in progress
    public static boolean recordingInProgress = false;
    // The videoDir variable is responsible for identifying the folder where the recordings
    // will be stored
    private String videoDir;

    /*
    *
    * This method generates a new intent for the recording service
    *
    * */
    static Intent newIntent(Context context, int resultCode, Intent data) {
        Intent intent = new Intent(context, RecordService.class);
        intent.putExtra(EXTRA_RESULT_CODE, resultCode);
        intent.putExtra(EXTRA_DATA, data);
        return intent;
    }

    /*
    *
    * The broadcast receiver is responsible for identifying when the device enters various states,
    * and handling the corresponding functionality
    *
    * */
    public class MyBroadcastReceiver extends BroadcastReceiver {
        @Override
        public void onReceive(Context context, Intent intent) {
            switch(intent.getAction()) {
                case Intent.ACTION_SCREEN_ON:
                    Log.i(TAG, "The device's screen is on: start recording");
                    startRecording(resultCode, data);
                    screenOff = false;
                    sendBroadcast(new Intent(context, NotifyInactiveReceiver.class)
                          .putExtra("INTENT_ACTION", "SCREEN_IS_ON"));
                    break;
                case Intent.ACTION_SCREEN_OFF:
                    Log.i(TAG, "The device's screen is off: stop recording and schedule the UploadJobService");
                    screenOff = true;
                    sendBroadcast(new Intent(context, NotifyInactiveReceiver.class)
                          .putExtra("INTENT_ACTION", "SCREEN_IS_OFF"));
                    stopRecording();
                    UploadJobService.scheduleJob(context);
                    break;
                case Intent.ACTION_CONFIGURATION_CHANGED:
                    Log.i(TAG, "The device's configuration has changed: restarting recording");
                    if (!screenOff) {
                        stopRecording();
                        startRecording(resultCode, data);
                    }
                    break;
                case Intent.ACTION_BATTERY_CHANGED:
                    if (isConnected(context)) {
                        UploadJobService.scheduleJob(context);
                    }
                    break;
            }
        }
    }

    /*
    *
    * This method determines if the device is charging
    *
    * */
    private static boolean isConnected(Context context) {
        Intent intent = context.registerReceiver(
              null, new IntentFilter(Intent.ACTION_BATTERY_CHANGED));
        int plugged = intent.getIntExtra(BatteryManager.EXTRA_PLUGGED, -1);
        return (plugged == BatteryManager.BATTERY_PLUGGED_AC
                    || plugged == BatteryManager.BATTERY_PLUGGED_USB);
    }

    /*
    *
    * The ServiceHandler is here applied to assist with messages involved in starting the recording
    * service
    *
    * */
    private final class ServiceHandler extends Handler {
        public ServiceHandler(Looper looper) {
            super(looper);
        }
        @Override
        public void handleMessage(Message msg) {
            if (resultCode == RESULT_OK) {
                startRecording(resultCode, data);
            }
        }
    }

    /*
    *
    * This method deals with the initiation events of the recording service
    *
    * */
    @Override
    public void onCreate() {
        // The service is instantiated in the foreground to prevent it from getting killed when the
        // app is closed
        Intent notificationIntent = new Intent(this, RecordService.class);
        PendingIntent pendingIntent = PendingIntent.getActivity(
              this, 0, notificationIntent, PendingIntent.FLAG_MUTABLE);
        // Attempt to generate the notification channel
        generateNotificationChannel(this, NOTIFICATION_RECORDING_CHANNEL_ID,
              NOTIFICATION_RECORDING_CHANNEL_ID_NAME, NOTIFICATION_RECORDING_CHANNEL_DESCRIPTION);
        // Send the notification
        NotificationCompat.Builder builderPeriodicNotification = constructNotification(this,
              NOTIFICATION_RECORDING_CHANNEL_ID,
              get_NOTIFICATION_RECORDING_TITLE(this),
              NOTIFICATION_RECORDING_DESCRIPTION)
              .setContentIntent(pendingIntent);
        Notification notification = constructNotificationForward(this, builderPeriodicNotification);
        // Configure and start the service
        // Forward/backwards compatibility
        startForeground(ONGOING_NOTIFICATION_ID, notification);
        // The receiver registers for determining if the device screen is on or off
        mScreenStateReceiver = new MyBroadcastReceiver();
        IntentFilter screenStateFilter = new IntentFilter();
        screenStateFilter.addAction(Intent.ACTION_SCREEN_ON);
        screenStateFilter.addAction(Intent.ACTION_SCREEN_OFF);
        screenStateFilter.addAction(Intent.ACTION_CONFIGURATION_CHANGED);
        screenStateFilter.addAction(Intent.ACTION_BATTERY_CHANGED);
        registerReceiver(mScreenStateReceiver, screenStateFilter);
        // Set the handler's operation to be conducted in the background
        HandlerThread thread = new HandlerThread("ServiceStartArguments",
                Process.THREAD_PRIORITY_BACKGROUND);
        thread.start();
        Looper mServiceLooper = thread.getLooper();
        mServiceHandler = new ServiceHandler(mServiceLooper);
        // The videoDir variable is set here
        videoDir = MainActivity.getMainDir(this.getApplicationContext()).getAbsolutePath()
                                            + (File.separatorChar + "videos" + File.separatorChar);
    }

    /*
    *
    * This method is executed when the service is started
    *
    * */
    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        // Get the intent
        resultCode = intent.getIntExtra(EXTRA_RESULT_CODE, 0);
        data = intent.getParcelableExtra(EXTRA_DATA);
        // If the intent is malformed, throw an error
        if (resultCode == 0 || data == null) {
            throw new IllegalStateException("Result code or data missing.");
        }
        // Send the message to the mServiceHandler
        Message msg = mServiceHandler.obtainMessage();
        msg.arg1 = startId;
        mServiceHandler.sendMessage(msg);
        return START_REDELIVER_INTENT;
    }
    
    /*
    * 
    * This method starts the media recorder, and generates the resulting video files
    * 
    * */
    private void startRecording(int resultCode, Intent data) {
        int videoRecordingMaximumFileSize = Settings.videoRecordingMaximumFileSize;
        int videoRecordingEncodingBitRate = Settings.videoRecordingEncodingBitRate;
        int videoRecordingFrameRate = Settings.videoRecordingFrameRate;
        // If the recording is not in progress
        if(!recordingInProgress) {
            // Set up a new MediaProjectionManager for the recording process
            MediaProjectionManager mProjectionManager = (MediaProjectionManager)
                  getApplicationContext().getSystemService(Context.MEDIA_PROJECTION_SERVICE);
            mMediaRecorder = new MediaRecorder();
            DisplayMetrics metrics = new DisplayMetrics();
            WindowManager wm = (WindowManager) getApplicationContext().getSystemService(WINDOW_SERVICE);
            wm.getDefaultDisplay().getRealMetrics(metrics);
            int mScreenDensity = metrics.densityDpi;
            int displayWidth = (int)Math.max(Math.round(metrics.widthPixels/Settings.recordScaleDivisor),500);
            int displayHeight = (int)Math.round(displayWidth*((double)metrics.heightPixels/(double)metrics.widthPixels));
            // Determine the orientation of the device
            String finalOrientation = ((displayWidth < displayHeight) ? "portrait" : "landscape");
            // The following info listener is set to execute when the mMediaRecorder identifies that
            // the recording service has created a video recording that exceeds the maximum file size
            mMediaRecorder.setOnInfoListener((mr, what, extra) -> {
                // If the maximum file size has been reached
                if (what == MediaRecorder.MEDIA_RECORDER_INFO_MAX_FILESIZE_APPROACHING) {
                    Log.i(TAG,"The media recorder has identified that the maximum file size has"
                          + " been reached; setting new output file.");
                    // Write out a new file
                    try (RandomAccessFile newRandomAccessFile =
                               new RandomAccessFile(videoDir + File.separatorChar + "time_"
                                     + System.currentTimeMillis() + "_mode_" + finalOrientation
                                     + ".mp4","rw")) {
                        mMediaRecorder.setNextOutputFile(newRandomAccessFile.getFD());
                    } catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            // Configure the mMediaRecorder
            mMediaRecorder.setVideoSource(MediaRecorder.VideoSource.SURFACE);
            mMediaRecorder.setOutputFormat(MediaRecorder.OutputFormat.MPEG_4);
            mMediaRecorder.setMaxFileSize(videoRecordingMaximumFileSize); // 5mb (4.7mb)
            mMediaRecorder.setVideoEncoder(MediaRecorder.VideoEncoder.H264);
            mMediaRecorder.setVideoEncodingBitRate(videoRecordingEncodingBitRate);
            mMediaRecorder.setVideoFrameRate(videoRecordingFrameRate);
            mMediaRecorder.setVideoSize(displayWidth, displayHeight);
            // Set the preliminary output file
            mMediaRecorder.setOutputFile(videoDir + File.separatorChar
                  + "time_" + System.currentTimeMillis() + "_mode_" + finalOrientation + ".mp4");
            try {
                // Attempt to prepare the recording
                mMediaRecorder.prepare();
                mMediaProjection = mProjectionManager.getMediaProjection(resultCode, data);
                Surface surface = mMediaRecorder.getSurface();
                mVirtualDisplay = mMediaProjection.createVirtualDisplay("MainActivity",
                      displayWidth, displayHeight, mScreenDensity,
                      VIRTUAL_DISPLAY_FLAG_PRESENTATION,
                      surface, null, null);
                // Start the recording
                mMediaRecorder.start();
                recordingInProgress = true;
                sendBroadcast(new Intent(this, NotifyInactiveReceiver.class)
                      .putExtra("INTENT_ACTION", "RECORDING_HAS_STARTED"));
            } catch (Exception e) {
                Log.e(TAG, "Failed on startRecording: ", e);
            }
        }
    }

    /*
    *
    * This method stops the recording service
    *
    * */
    private void stopRecording() {
        // If the recording is in progress
        if (recordingInProgress) {
            // Attempt to stop the service
            boolean actionedStop = true;
            try {
                mMediaRecorder.stop();
            } catch(Exception e) {
                actionedStop = false;
                Log.e(TAG, "Failed on stopRecording: ", e);
            }
            try {
                mMediaProjection.stop();
            } catch(Exception e) {
                actionedStop = false;
                Log.e(TAG, "Failed on stopRecording: ", e);
            }
            try {
                mMediaRecorder.release();
            } catch(Exception e) {
                actionedStop = false;
                Log.e(TAG, "Failed on stopRecording: ", e);
            }
            try {
                mVirtualDisplay.release();
            } catch(Exception e) {
                actionedStop = false;
                Log.e(TAG, "Failed on stopRecording: ", e);
            }
            if (actionedStop) {
                recordingInProgress = false;
                sendBroadcast(new Intent(this, NotifyInactiveReceiver.class)
                      .putExtra("INTENT_ACTION", "RECORDING_HAS_STOPPED"));
            }
        }
    }

    /*
    *
    * This method is executed on binding the recording service
    *
    * */
    @Override
    public IBinder onBind(Intent intent) {
        // There is no binding, so return null
        return null;
    }

    /*
    *
    * This method is executed on destroying the service
    *
    * */
    @Override
    public void onDestroy() {
        stopRecording();
        unregisterReceiver(mScreenStateReceiver);
        stopSelf();
    }
}
