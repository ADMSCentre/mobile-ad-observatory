package com.adms.australianmobileadobservatory;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_PERIODIC_CHANNEL_DESCRIPTION;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_PERIODIC_CHANNEL_ID;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_PERIODIC_CHANNEL_ID_NAME;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_PERIODIC_DESCRIPTION;
import static com.adms.australianmobileadobservatory.Settings.NOTIFICATION_REBOOT_DESCRIPTION;
import static com.adms.australianmobileadobservatory.Settings.get_NOTIFICATION_PERIODIC_TITLE;
import static com.adms.australianmobileadobservatory.Settings.get_NOTIFICATION_REBOOT_TITLE;
import static com.adms.australianmobileadobservatory.Settings.intervalMillisecondsBetweenPeriodicNotifications;
import static com.adms.australianmobileadobservatory.Settings.sharedPreferenceGet;
import static com.adms.australianmobileadobservatory.Settings.sharedPreferencePut;
import android.app.AlarmManager;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.content.res.Configuration;
import android.icu.util.Calendar;
import android.util.Log;
import androidx.core.app.ActivityCompat;
import androidx.core.app.NotificationCompat;
import androidx.core.app.NotificationManagerCompat;
import androidx.core.content.ContextCompat;

import java.util.Objects;

public class NotifyInactiveReceiver extends BroadcastReceiver {

   private static String TAG = "NotifyInactiveReceiver";

   /*
   *
   * This function is responsible for setting the internal alarm associated with ensuring that periodic
   * notifications are regularly sent to the user when they are not running the app in the background
   *
   * */
   public static void setPeriodicNotifications(Context context) {
      if (sharedPreferenceGet(context, "SHARED_PREFERENCE_PERIODIC_NOTIFICATIONS_SET", "false").equals("false")) {
         Log.i(TAG, "Attempted to set periodic notifications: success");
         Intent notificationIntent = new Intent(context, NotifyInactiveReceiver.class);
         notificationIntent.putExtra("INTENT_ACTION", "PERIODIC_NOTIFICATION");
         final PendingIntent pendingIntent = PendingIntent.getBroadcast(context, 0,
               notificationIntent, PendingIntent.FLAG_NO_CREATE | PendingIntent.FLAG_IMMUTABLE);
         if (pendingIntent == null) {
            PendingIntent pending = PendingIntent.getBroadcast(context, 0, notificationIntent,
                  PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
            // Set the time of both the initial (and recurring) periodic notifications
            // Note that the first instance will occur intervalMillisecondsBetweenPeriodicNotifications*2
            // milliseconds into the future
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis() + intervalMillisecondsBetweenPeriodicNotifications);
            ((AlarmManager) context.getSystemService(Context.ALARM_SERVICE)).setRepeating(
                  AlarmManager.RTC, Calendar.getInstance().getTime().toInstant().toEpochMilli(),
                  intervalMillisecondsBetweenPeriodicNotifications, pending);
         }
         sharedPreferencePut(context, "SHARED_PREFERENCE_PERIODIC_NOTIFICATIONS_SET", "true");
      } else {
         Log.i(TAG, "Attempted to set periodic notifications: already set");
      }
   }

   private static boolean isDarkTheme(Context context) {
      int nightModeFlags = context.getResources().getConfiguration().uiMode & Configuration.UI_MODE_NIGHT_MASK;
      return nightModeFlags == Configuration.UI_MODE_NIGHT_YES;
   }

   /*
   *
   * This method is responsible for handling consistent stylisation of notifications that are sent out
   * through the app
   *
   * */
   public static NotificationCompat.Builder constructNotification(Context context, String channelID, String title, String text) {
      NotificationCompat.Builder thisNotificationBuilder = new NotificationCompat.Builder(context, channelID)
            .setSmallIcon(R.mipmap.ic_stat_adaptive) // entirely white
            .setContentTitle(title)
            .setContentText(text)
            .setColor(ContextCompat.getColor(context, R.color.colorNotificationLightMode))
            .setPriority(NotificationCompat.PRIORITY_DEFAULT);
      if (isDarkTheme(context)) {
         thisNotificationBuilder.setColor(ContextCompat.getColor(context, R.color.colorNotificationDarkMode));
      }
      /*
      switch (context.getResources().getConfiguration().uiMode & Configuration.UI_MODE_NIGHT_MASK) {
         case Configuration.UI_MODE_NIGHT_YES:
            thisNotificationBuilder.setColor(ContextCompat.getColor(context, R.color.colorNotificationDarkMode));
            break;
         case Configuration.UI_MODE_NIGHT_NO:
            thisNotificationBuilder.setColor(ContextCompat.getColor(context, R.color.colorNotificationLightMode));
            break;
         case Configuration.UI_MODE_NIGHT_UNDEFINED:
            thisNotificationBuilder.setColor(ContextCompat.getColor(context, R.color.colorNotificationLightMode));
            break;
      }*/
      return thisNotificationBuilder;
   }

   /*
    *
    * This method is responsible for handling intermittent build aspects of the notification construction
    *
    * */
   public static Notification constructNotificationForward(Context context, NotificationCompat.Builder builder) {
      NotificationManagerCompat notificationManager = NotificationManagerCompat.from(context);
      /*if (ActivityCompat.checkSelfPermission(context,
            android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
         // We don't request permissions here - this is for the user to do when they use the app.
         Log.i(TAG, "Permission not registering");
         return null;
      }*/ // This has been commented out because it can be auto-handled by general notification behaviour
      Notification builtNotification = builder.build();
      return builtNotification;
   }



   /*
   *
   * This method is responsible for sending notifications
   *
   * */
   public static Notification sendNotification(Context context, NotificationCompat.Builder builder) {
      NotificationManagerCompat notificationManager = NotificationManagerCompat.from(context);
      if (ActivityCompat.checkSelfPermission(context,
            android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
         // We don't request permissions here - this is for the user to do when they use the app.
         return null;
      }
      Notification builtNotification = builder.build();
      notificationManager.notify(1, builtNotification);
      return builtNotification;
   }

   /*
   *
   * This method generates the notification channel, on which reminders to re-enable the app are
   * broadcasted - the channel can be re-declared repeatedly without any side effects, but MUST
   * be instantiated before any notifications are sent off to allow successful transmission.
   * It is thus instantiated both within this class's onRecieve method, and within the onCreate
   * method of the MainActivity class.
   *
   * */
   public static void generateNotificationChannel(Context context, String channelID, String channelName, String channelDescription) {
      // Generate the notification channel
      NotificationChannel channel = new NotificationChannel(
            channelID, channelName, NotificationManager.IMPORTANCE_DEFAULT);
      channel.setDescription(channelDescription);
      context.getSystemService(NotificationManager.class).createNotificationChannel(channel);
   }

   /*
   *
   * This method determines whether the user should be notified of the app's inactivity
   *
   * */
   private boolean shouldUserBeNotifiedAboutInactivity(Context context) {
      return (!sharedPreferenceGet(context, "RECORDING_STATUS", "false").equals("true"));
   }

   /*
   *
   * // TODO - only send if the app is not recording
   * This method receives intents in two cases:
   *     1. The device has rebooted: In this case, the app automatically stops recording because it
   *        no longer has permission to record the screen (note that it is impossible to persist
   *        permissions between boots, as the intents that carry the permissions are serialised using
   *        Parcel, which facilitates information transfer that can't be read off disk. Upon reboot,
   *        we designate three actions: the first action is that a perpetual alarm is set to fire off
   *        periodic notifications to ensure that the user is aware when the app is not recording;
   *        the second action is that a one-time notification is also sent to the user to let them know
   *        that the app has stopped recording; the third action is that the user is directed to the
   *        app (provided that certain device-specific conservation features are met).
   *     2. The periodic alarm has been fired: In this case, the aforementioned periodic
   *        notification is sent to the user.
   *
   * */
   @Override
   public void onReceive(Context context, Intent intent) {
      // Set the periodic alarm that is called to determine if the software is observing ads or not
      // We place it for any received event, as we try to maximise the instances in which it is called
      setPeriodicNotifications(context);
      try {
         // Attempt to generate the notification channel
         generateNotificationChannel(context, NOTIFICATION_PERIODIC_CHANNEL_ID,
               NOTIFICATION_PERIODIC_CHANNEL_ID_NAME, NOTIFICATION_PERIODIC_CHANNEL_DESCRIPTION);
         // If the device has rebooted:
         if ((intent != null) && (intent.getAction() != null) && (intent.getAction().equals(Intent.ACTION_BOOT_COMPLETED))) {
            Log.i(TAG, "'SCREEN_OFF_DURING_RECORDING' and 'RECORDING_STATUS' variables have been set back to 'false'");
            // Reset the 'SCREEN_OFF_DURING_RECORDING' and 'RECORDING_STATUS' variables
            sharedPreferencePut(context, "SCREEN_OFF_DURING_RECORDING", "false");
            sharedPreferencePut(context, "RECORDING_STATUS", "false");
            // Reset the periodic notifications indicator (so that it isn't doubly set)
            sharedPreferencePut(context, "SHARED_PREFERENCE_PERIODIC_NOTIFICATIONS_SET", "false");
            // If the app is not recording after a reboot...
            if (shouldUserBeNotifiedAboutInactivity(context)) {
               // Attempt to start the app - TODO - to the main screen with a directive
               Intent i = new Intent(context, MainActivity.class);
               i.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
               i.setAction(android.provider.Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS);
               context.startActivity(i);
               // Send the reboot notification informing the user that the app is not observing ads
               NotificationCompat.Builder builderRebootNotification = constructNotification(context,
                     NOTIFICATION_PERIODIC_CHANNEL_ID,
                     get_NOTIFICATION_REBOOT_TITLE(context),
                     NOTIFICATION_REBOOT_DESCRIPTION);
               sendNotification(context, builderRebootNotification);
            }
         } else
         // If the periodic alarm has been set off:
         if ((intent != null) && (intent.hasExtra("INTENT_ACTION"))
            && (intent.getStringExtra("INTENT_ACTION").equals("PERIODIC_NOTIFICATION"))) {
               Log.i(TAG, "Periodic inactivity notification intent has been received");
               // If the app is not recording periodically...
               if (shouldUserBeNotifiedAboutInactivity(context)) {
                  Log.i(TAG, "Periodic inactivity notification has been fired");
                  // Send the periodic notification to inform the user that the app is not observing ads
                  NotificationCompat.Builder builderPeriodicNotification = constructNotification(context,
                        NOTIFICATION_PERIODIC_CHANNEL_ID,
                        get_NOTIFICATION_PERIODIC_TITLE(context),
                        NOTIFICATION_PERIODIC_DESCRIPTION);
                  sendNotification(context, builderPeriodicNotification);
               } else {
                  Log.i(TAG, "Periodic inactivity notification has been rejected - the screen recording is running");
               }
         } else
         // If the screen recording has started
         if ((intent != null) && (intent.hasExtra("INTENT_ACTION"))
            && (intent.getStringExtra("INTENT_ACTION").equals("RECORDING_HAS_STARTED"))) {
               Log.i(TAG, "RECORDING_HAS_STARTED");
               sharedPreferencePut(context, "RECORDING_STATUS", "true");
         } else
         // If the screen recording has stopped
         if ((intent != null) && (intent.hasExtra("INTENT_ACTION"))
            && (intent.getStringExtra("INTENT_ACTION").equals("RECORDING_HAS_STOPPED"))) {
               Log.i(TAG, "RECORDING_HAS_STOPPED");
               // If the recording stops after a registered screen off event
               if (sharedPreferenceGet(context, "SCREEN_OFF_DURING_RECORDING", "false").equals("true")) {
                  // Do nothing, as we know that the screen is off during a recording session
               } else {
                  sharedPreferencePut(context, "RECORDING_STATUS", "false");
               }
         } else
         // If the screen is on
         if ((intent != null) && (intent.hasExtra("INTENT_ACTION"))
            && (intent.getStringExtra("INTENT_ACTION").equals("SCREEN_IS_ON"))) {
               Log.i(TAG, "SCREEN_IS_ON");
               // By extension of that the screen is on, the screen being off during a recording is falsified
               sharedPreferencePut(context, "SCREEN_OFF_DURING_RECORDING", "false");
         } else
         // If the screen is off
         if ((intent != null) && (intent.hasExtra("INTENT_ACTION"))
            && (intent.getStringExtra("INTENT_ACTION").equals("SCREEN_IS_OFF"))) {
               Log.i(TAG, "SCREEN_IS_OFF");
               // If the screen switches off during a recording
               if (sharedPreferenceGet(context, "RECORDING_STATUS", "false").equals("true")) {
                  sharedPreferencePut(context, "SCREEN_OFF_DURING_RECORDING", "true");
               }
         }
      } catch (Exception e) {
         // Do nothing
         Log.i("onReceive has failed: ", String.valueOf(e));
      }
   }
}


