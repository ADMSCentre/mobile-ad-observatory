/*
 *
 * This class is the entry point and main routine of the app
 *
 * */

package com.adms.australianmobileadobservatory;

import static android.Manifest.permission.POST_NOTIFICATIONS;
import static com.adms.australianmobileadobservatory.LogoDetector.getDeviceIdentifier;
import static com.adms.australianmobileadobservatory.NotifyInactiveReceiver.generateNotificationChannel;
import static com.adms.australianmobileadobservatory.NotifyInactiveReceiver.setPeriodicNotifications;
import static com.adms.australianmobileadobservatory.OCRManager.closeOCR;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_PERIODIC_CHANNEL_DESCRIPTION;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_PERIODIC_CHANNEL_ID;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_PERIODIC_CHANNEL_ID_NAME;
import static com.adms.australianmobileadobservatory.Settings.SHARED_PREFERENCE_OBSERVER_ID_DEFAULT_VALUE;
import static com.adms.australianmobileadobservatory.Settings.SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE;
import static com.adms.australianmobileadobservatory.Settings.sharedPreferenceGet;
import static com.adms.australianmobileadobservatory.Settings.sharedPreferencePut;

import android.annotation.SuppressLint;
import android.app.ActivityManager;
import android.app.FragmentTransaction;
import android.app.NotificationManager;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.content.res.Resources;
import android.media.projection.MediaProjectionManager;
import android.os.Build;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.Switch;

import androidx.annotation.RequiresApi;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;
import androidx.fragment.app.Fragment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// TODO do further testing on intermittent stops

public class MainActivity extends BaseActivity {
    private static final String TAG = "MainActivity";
    // The permission code necessary for screen-recording
    public static final int PERMISSION_CODE = 1;
    // The MediaProjectionManager used with screen-recording
    public static MediaProjectionManager mProjectionManager;
    // The main directory variable (to be used with file copying)
    private static File mainDir;
    // The device identifier
    public static String thisDeviceIdentifier = getDeviceIdentifier(android.os.Build.MODEL);
    // The observer ID is set to nothing to begin
    public static String THIS_OBSERVER_ID = SHARED_PREFERENCE_OBSERVER_ID_DEFAULT_VALUE;
    // The registration status of the user
    public static String THIS_REGISTRATION_STATUS = SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE;

    /*
     *
     * This method assists with creating new configuration details for devices
     *
     * */
    public void logDeviceIdentifier() {
        Log.i(TAG, "Device Identifier: " + android.os.Build.MODEL);
    }

    /*
     *
     * This method is called anytime the app spins up
     *
     * */
    @RequiresApi(api = Build.VERSION_CODES.TIRAMISU)
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // The directories that will need to be created
        String[] directoriesToCreate = Settings.DIRECTORIES_TO_CREATE;
        // Set the main view
        setContentView(R.layout.activity_base);
        mProjectionManager = (MediaProjectionManager) getSystemService(Context.MEDIA_PROJECTION_SERVICE);

        // Determine whether the service is running, and use this to determine whether the mToggleButton
        // is then checked
        FragmentMain.setToggle(isServiceRunning());
        // Identify the main directory of the app
        mainDir = getMainDir(this.getApplicationContext());
        //prefs.edit().clear().commit(); // Uncomment this line to wipe the Shared Preferences
        // (in case it doesn't wipe when clearing the cache and storage, which technically shouldn't
        // happen, but here we are)
        // Run this block on the first run of the app
        if (sharedPreferenceGet(this, "SHARED_PREFERENCE_FIRST_RUN", "true").equals("true")) {
            Log.i(TAG, "First run: setting shared preferences and generating directories");
            // Create the directory required by the app within the mainDir
            if ((!mainDir.exists()) && (!mainDir.mkdirs())) {
                Log.e(TAG, "Failure on onCreate: couldn't create main directory");
            }
            // Create the directories that are necessary for the app's functionality
            for (String value : directoriesToCreate) {
                File dir = new File(mainDir
                      + (File.separatorChar + value + File.separatorChar));
                // Fail-safe (in case the directory already exists)
                if ((!dir.exists()) && (!dir.mkdirs())) {
                    Log.e(TAG, "Failure on onCreate: couldn't create sub-directories");
                }
            }
            // Copy across the OCR data into the necessary folder
            copyOCRTesseractData();
            // Generate an observer ID for this device, to be later submitted with data donations
            sharedPreferencePut(this, "SHARED_PREFERENCE_OBSERVER_ID", UUID.randomUUID().toString());
            // This code block has finished - commit the SHARED_PREFERENCE_FIRST_RUN variable,
            // to ensure it doesn't run again
            sharedPreferencePut(this, "SHARED_PREFERENCE_FIRST_RUN", "false");
        }
        // Set the observer ID
        THIS_OBSERVER_ID = sharedPreferenceGet(this, "SHARED_PREFERENCE_OBSERVER_ID",
              SHARED_PREFERENCE_OBSERVER_ID_DEFAULT_VALUE);
        THIS_REGISTRATION_STATUS = sharedPreferenceGet(this, "SHARED_PREFERENCE_REGISTERED",
              SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE);

        // Retrieve permission to send notifications whenever the app is opened
        if (ContextCompat.checkSelfPermission(MainActivity.this, POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(MainActivity.this, new String[]{POST_NOTIFICATIONS},101);
        }
        // Generate the notification channel
        generateNotificationChannel(this, NOTIFICATION_PERIODIC_CHANNEL_ID,
              NOTIFICATION_PERIODIC_CHANNEL_ID_NAME, NOTIFICATION_PERIODIC_CHANNEL_DESCRIPTION);
        // Attempt to set the periodic notifications
        setPeriodicNotifications(this);
    }

    /*
     *
     * This method determines the location of the mainDir variable, depending on internal/external
     * storage configurations (NOTE: Functionality has been removed due to updated permissions on newer
     * Android SDKs)
     *
     * */
    public static File getMainDir(Context context) {
        // The child directory to instantiate
        String childDirectory = Settings.APP_CHILD_DIRECTORY;
        // Determine the external files directories
        File[] externalFilesDirs = ContextCompat.getExternalFilesDirs(context, null);
        // If an SD card is detected, use it; otherwise use the internal storage
        return new File(externalFilesDirs[0], childDirectory);
    }

    /*
     *
     * This method copies across the Google Tesseract API data to a device storage location where it
     * can be used
     *
     * */
    private void copyOCRTesseractData() {
        // The source folder of the training data files
        String trainingDataFilesSourceDir = Settings.TRAINING_DATA_FILES_SOURCE_DIRECTORY;
        // Index the training data files, and generate file IDs for each of them
        List<Integer> fileIds = new ArrayList<>();
        for (Field f : R.raw.class.getFields()) {
            @SuppressLint("DiscouragedApi")
            int id = getResources().getIdentifier(
                  f.getName(), trainingDataFilesSourceDir, getPackageName());
            if (id > 0) {
                fileIds.add(id);
            }
        }
        // Attempt to create the necessary files
        try {
            createFiles(this.getBaseContext(), fileIds);
        } catch (Exception e) {
            Log.e(TAG, "Failed on : copyOCRTesseractData", e);
        }
    }

    /*
     *
     * This method attempts to create a list of files
     *
     * */
    public static void createFiles(final Context context, final List<Integer> inputRawResources) {
        try {
            // Get the context's resources
            final Resources resources = context.getResources();
            // Number of bytes to read in a single chunk
            final byte[] largeBuffer = new byte[1024 * 4];
            // Ephemeral variable used for tracking bytes to allocate for each file
            int bytesRead = 0;
            // For each file
            for (Integer resource : inputRawResources) {
                String fName = resources.getResourceEntryName(resource)
                      .substring(9)
                      .replace("_", ".")
                      .replace("0", "-");
                File outFile = new File(mainDir.getAbsolutePath()
                      + (File.separatorChar + "tessdata" + File.separatorChar), fName);
                // Read the file as a stream, and allocate it
                final OutputStream outputStream = new FileOutputStream(outFile);
                final InputStream inputStream = resources.openRawResource(resource);
                while ((bytesRead = inputStream.read(largeBuffer)) > 0) {
                    if (largeBuffer.length == bytesRead) {
                        outputStream.write(largeBuffer);
                    } else {
                        final byte[] shortBuffer = new byte[bytesRead];
                        System.arraycopy(largeBuffer, 0, shortBuffer, 0, bytesRead);
                        outputStream.write(shortBuffer);
                    }
                }
                inputStream.close();
                outputStream.flush();
                outputStream.close();
            }
        } catch (Exception e) {
            Log.e(TAG, "Failed on createFiles: ", e);
        }

    }

    /*
     *
     * This method handles the functionality of resuming the app
     *
     * */
    @Override
    public void onResume() {
        super.onResume();
        FragmentMain.setToggle(isServiceRunning());
    }

    /*
     *
     * This method handles activity results (e.g. getting permission for screen-recording)
     *
     * */
    @Override
    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        // Inform us if the activity code doesn't correspond to a permission request
        if (requestCode != PERMISSION_CODE) {
            Log.e(TAG, "Unknown request code: " + requestCode);
            return;
        }
        // If given permission to record the device, begin recording
        if (resultCode == RESULT_OK) {
            startRecordingService(resultCode, data);
        } else {
            // The mToggleButton must be forced off in case the permission request fails
            FragmentMain.setToggle(false);
        }
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        closeOCR();
        sendBroadcast(new Intent(this, NotifyInactiveReceiver.class)
              .putExtra("INTENT_ACTION", "APP_HAS_CLOSED"));
    }

    /*
     *
     * This method starts the screen-recording
     *
     * */
    private void startRecordingService(int resultCode, Intent data) {
        Intent intent = RecordService.newIntent(this, resultCode, data);
        startService(intent);
    }

    /*
     *
     * This method determines whether the service associated with the app is running, mainly for use
     * with setting the toggle button 'on'
     *
     * */
    public boolean isServiceRunning() {
        // Get the ActivityManager for the device
        ActivityManager manager = (ActivityManager) getSystemService(Context.ACTIVITY_SERVICE);
        // Loop through all services within it
        for (ActivityManager.RunningServiceInfo service : manager.getRunningServices(Integer.MAX_VALUE)) {
            // If any of the services are equal to that of this app, return true
            if (RecordService.class.getName().equals(service.service.getClassName())) {
                return true;
            }
        }
        return false;
    }
}

