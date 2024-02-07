package com.adms.australianmobileadobservatory;

import android.content.Context;
import android.content.SharedPreferences;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import android.content.pm.ApplicationInfo;

public class Settings {
   // The extra result code associated with the intent of the recording service
   public static final String RECORD_SERVICE_EXTRA_RESULT_CODE = "AustralianMobileAdObservatoryExtraResultCode";
   public static final String RECORD_SERVICE_EXTRA_DATA = "data";
   // The ID of the notification associated with the recording service
   // (the value has no actual bearing on the functionality, although don't set it to zero:
   // https://developer.android.com/guide/components/foreground-services#:~:text=startForeground(ONGOING_NOTIFICATION_ID%2C%20notification)%3B)
   public static final int RECORD_SERVICE_ONGOING_NOTIFICATION_ID = 1;
   // Maximum file size for video recordings (5MB)
   public static int videoRecordingMaximumFileSize = 5000000;
   // Video recording encoding bit rate
   public static int videoRecordingEncodingBitRate = 250000;
   // Video recording frame rate
   public static int videoRecordingFrameRate = 6;
   // The upload job service ID
   public static final int jobServiceID = 999;
   public static double recordScaleDivisor = 2.0;



   // The directories that will need to be created
   //         "tessdata" (Required by Google Tesseract API);
   //         "temp" (Required by OCR Manager);
   //         "videos" (Required by screen-recorder)
   public static String[] DIRECTORIES_TO_CREATE = {"videos", "temp", "tessdata"};
   // The child directory to instantiate for the app
   public static String APP_CHILD_DIRECTORY = "australianmobileadobservatory";
   // The source folder of the training data files
   public static String TRAINING_DATA_FILES_SOURCE_DIRECTORY = "raw";
   public static String AWS_LAMBDA_ENDPOINT = "https://nmzoodzqpiuok4adbqvldcog4y0mlumv.lambda-url.us-east-2.on.aws/";
   public static int IMAGE_EXPORT_QUALITY = 100; // TODO - quality is already reduced at this point
   // The amount to compress the image during conversion
   public static int IMAGE_CONVERSION_QUALITY = 90;
   public static String IDENTIFIER_DATA_DONATION = "DATA_DONATION";
   public static String IDENTIFIER_REGISTRATION = "REGISTRATION";
   public static int AWS_LAMBDA_ENDPOINT_CONNECTION_TIMEOUT = 30000;
   public static int AWS_LAMBDA_ENDPOINT_READ_TIMEOUT = 30000;

   public static int IMAGE_SIMILARITY_SCALE_PIXELS_WIDTH = 20;

   public static int RECORDER_FRAME_INTERVALS = 10;

   public static int RECORDER_FRAME_POSITIVE_COOLDOWN = 3;

   public static double RECORDER_FRAME_SIMILARITY_THRESHOLD = 0.9;

   public static double LOGO_MAX_INTERSECTION_PERCENTAGE = 0;
   public static double OCRImageCropFromLeft = 0.3;
   public static double recordScaleDivisorOCRUpscale = 1.5;

   public static boolean[][] LOGO_BOOLEAN_PICTOGRAM_MATRIX = {
         { false, false, false, false, false, false, false, false, false, false},
         { false, false, false, false,  true,  true, false, false, false, false},
         { false, false, false,  true,  true,  true,  true, false, false, false},
         { false, false,  true,  true,  true,  true,  true,  true, false, false},
         { false,  true,  true,  true,  true,  true,  true,  true,  true, false},
         { false, false,  true,  true,  true,  true,  true,  true, false, false},
         { false, false,  true,  true, false, false,  true,  true, false, false},
         { false, false,  true,  true, false, false,  true,  true, false, false},
         { false, false,  true,  true, false, false,  true,  true, false, false},
         { false, false, false, false, false, false, false, false, false, false},
   };

   public static boolean[][] LOGO_BOOLEAN_PICTOGRAM_NOTIFICATION_MATRIX = {
         { false, false, false, false, false, false, true,  true,  true,  false},
         { false, false, false, false, false, true,  true,  true,  true,  true },
         { false, false, false, false, false, true,  true,  true,  true,  true },
         { false, false, false, false, false, true,  true,  true,  true,  true },
         { false, false, false, false, false, false, true,  true,  true,  false},
         { false, false, false, false, false, false, false, false, false, false},
         { false, false, false, false, false, false, false, false, false, false},
         { false, false, false, false, false, false, false, false, false, false},
         { false, false, false, false, false, false, false, false, false, false},
         { false, false, false, false, false, false, false, false, false, false},
   };

   /*
    *
    * This function generates a single detail (relative to a single UI colour scheme) of
    * the configuration used for evaluating the presence of logos within a viewport.
    *
    * @param identifier                         The identifier that distinguishes this
    *                                           configuration detail.
    *
    * @param anticipatedLogoDiameter            The anticipated diameter of the logo
    *                                           that is expected to be identified.
    *
    * @param anticipatedOriginX                 The anticipated X coordinate within
    *                                           which to begin searching for the logo.
    *
    * @param anticipatedOriginY                 The anticipated Y coordinate within
    *                                           which to begin searching for the logo.
    *
    * @param anticipatedViewportW               The anticipated viewport width.
    *
    * @param anticipatedViewportH               The anticipated viewport height.
    *
    * @param jitter                             The number of pixels that it is expected
    *                                           the logo will either be offset forward
    *                                           or backward.
    *
    * @param stride                             The number of pixels to move, either
    *                                           horizontally or vertically at each
    *                                           interval when searching for the logo.
    *
    * @param positiveReadingConsistencyAnchor   The ideal value of the positive readings
    *                                           consistency.
    *
    * @param negativeReadingConsistencyAnchor   The ideal value of the negative readings
    *                                           consistency.
    *
    * @param readingMaxDifference               The maximum difference in units that
    *                                           either the positive or negative readings
    *                                           for the logo's pictogram can differ from
    *                                           the consistency anchor.
    *
    * @param colorDistanceAnchor                The ideal distance of the contrast between
    *                                           the colors of positive and negative readings.
    *
    * @param colorMaxDifference                 The maximum difference in units that
    *                                           the contrast of the colors for the positive
    *                                           and negative readings can have against the
    *                                           color distance anchor.
    *
    * */
   public static JSONObject configurationDetail(
         String identifier,
         String deviceBuildModel,
         double downScale,
         int anticipatedLogoDiameter,
         int anticipatedOriginX,
         int anticipatedOriginY,
         int anticipatedOriginXOffset,
         int anticipatedOriginYOffset,
         int anticipatedViewportW,
         int anticipatedViewportH,
         int jitter,
         int stride,
         int positiveReadingConsistencyAnchor,
         int positiveReadingConsistencyAnchorDark,
         int negativeReadingConsistencyAnchor,
         int negativeReadingConsistencyAnchorDark,
         int readingMaxDifference,
         int colorDistanceAnchor,
         int colorDistanceAnchorDark,
         int colorMaxDifference) throws JSONException {
      JSONObject configurationPixel7 = new JSONObject();
      configurationPixel7.put("identifier",identifier);
      configurationPixel7.put("deviceBuildModel",deviceBuildModel);
      configurationPixel7.put("downScale",downScale);
      configurationPixel7.put("anticipatedOriginX",anticipatedOriginX);
      configurationPixel7.put("anticipatedOriginY",anticipatedOriginY);
      configurationPixel7.put("anticipatedOriginXOffset",anticipatedOriginXOffset);
      configurationPixel7.put("anticipatedOriginYOffset",anticipatedOriginYOffset);
      configurationPixel7.put("anticipatedLogoDiameter",anticipatedLogoDiameter);
      configurationPixel7.put("anticipatedViewportW",(double)anticipatedViewportW);
      configurationPixel7.put("anticipatedViewportH",(double)anticipatedViewportH);
      configurationPixel7.put("jitter",jitter);
      configurationPixel7.put("stride", stride);
      configurationPixel7.put("positiveReadingConsistencyAnchor",
            positiveReadingConsistencyAnchor);
      configurationPixel7.put("negativeReadingConsistencyAnchor",
            negativeReadingConsistencyAnchor);
      configurationPixel7.put("positiveReadingConsistencyAnchorDark",
            positiveReadingConsistencyAnchorDark);
      configurationPixel7.put("negativeReadingConsistencyAnchorDark",
            negativeReadingConsistencyAnchorDark);
      configurationPixel7.put("readingMaxDifference",readingMaxDifference);
      configurationPixel7.put("colorDistanceAnchor",colorDistanceAnchor);
      configurationPixel7.put("colorDistanceAnchorDark",colorDistanceAnchorDark);
      configurationPixel7.put("colorMaxDifference",colorMaxDifference);

      return configurationPixel7;
   }

   // TODO - shift the configurations of the accepted devices and privde how-to
   public static JSONObject deviceConfigurationDetails() {
      JSONObject thisDeviceConfigurationDetailsAsObject = new JSONObject();
      JSONArray thisDeviceConfigurationDetailsAsArray = new JSONArray();
      try {
         thisDeviceConfigurationDetailsAsArray.put(configurationDetail(
               "pixel_7_pro",
               "Pixel 7 Pro",
               0.20,
               70,
               55,
               134,
               55,
               271,
               1080,
               2340,
               4,
               1,
               46,
               43,
               57,
               47,
               45,
               190,
               145,
               60));
         thisDeviceConfigurationDetailsAsArray.put(configurationDetail(
               "galaxy_s22",
               "Galaxy S22",
               0.20,
               76,
               52,
               112,
               70,
               268,
               1080,
               2340,
               4,
               1,
               46,
               43,
               57,
               47,
               45,
               190,
               120,
               63));

         for (int i = 0; i < thisDeviceConfigurationDetailsAsArray.length(); i ++) {
            JSONObject thisDeviceConfigurationDetail = thisDeviceConfigurationDetailsAsArray.getJSONObject(i);
            thisDeviceConfigurationDetailsAsObject.put((String)thisDeviceConfigurationDetail.get("identifier"),thisDeviceConfigurationDetail);
         }
      } catch (JSONException e) {
         throw new RuntimeException(e);
      }
      return thisDeviceConfigurationDetailsAsObject;
   }
   // The suffixes that correspond to the various sub-images that may commmunicate a Facebook News
   // Feed logo pictogram
   public static final String[] logoDetectionTestSuffixes = {
         "light",
         "light_facebook_logo_offset",
         "light_notification",
         "light_facebook_logo_offset_notification",
         "dark",
         "dark_facebook_logo_offset",
         "dark_notification",
         "dark_facebook_logo_offset_notification",
         "light_negative",
         "dark_negative",
         "light_facebook_logo_offset_negative",
         "dark_facebook_logo_offset_negative"
   };
   // The number of executions to run within the unit test (multiply by number of sub-image variations to get
   // exact number of individual tests
   public static int numberOfExecutions = 1000;

   /*
    *
    * This method retrieves the name of the application
    *
    * */
   public static String getApplicationName(Context context) {
      ApplicationInfo applicationInfo = context.getApplicationInfo();
      int stringId = applicationInfo.labelRes;
      return stringId == 0 ? applicationInfo.nonLocalizedLabel.toString() : context.getString(stringId);
   }

   /*
   *
   * This method retrieves the title of the notification that is sent off whenever a reboot of the
   * device takes place
   *
   * */
   public static String get_NOTIFICATION_REBOOT_TITLE(Context context) {
      return "The "+getApplicationName(context)+" has stopped observing ads";
   }

   // The description of the notification that is sent off whenever a reboot of the device takes place
   public static String NOTIFICATION_REBOOT_DESCRIPTION =
         "Our app stops whenever your device reboots - you'll need to provide your permission to re-enable it";

   /*
    *
    * This method retrieves the title of the notification that is sent off periodically, when the device
    * is not observing ads
    *
    * */
   public static String get_NOTIFICATION_PERIODIC_TITLE(Context context) {
      return "The "+getApplicationName(context)+" is not recording";
   }

   // The description of the notification that is sent off periodically, when the device is not observing ads
   public static String NOTIFICATION_PERIODIC_DESCRIPTION =
         "You'll need to provide your permission to enable it";
   // The unique ID associated with the periodic notification channel
   public static String NOTIFICATION_PERIODIC_CHANNEL_ID = "adms_mobile_ad_observatory_notification_periodic_channel";
   // The front-facing name associated with the periodic notification channel
   public static String NOTIFICATION_PERIODIC_CHANNEL_ID_NAME = "Inactivity Reminders";
   // The front-facing description associated with the periodic notification channel
   public static String NOTIFICATION_PERIODIC_CHANNEL_DESCRIPTION = "Our app is designed to stop whenever your device restarts. When this happens, we'll need your permission to re-enable it";
   // The interval (in milliseconds) between periodic notifications
   public static int intervalMillisecondsBetweenPeriodicNotifications = 1000 * 30 * 1; // TODO adjust - make it start after given amount of time
   // The default value of the observer ID
   public static String SHARED_PREFERENCE_OBSERVER_ID_DEFAULT_VALUE = "undefined";
   // The default value of the registrationStatus
   public static String SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE = "undefined";

   // The unique ID associated with the periodic notification channel
   public static String NOTIFICATION_RECORDING_CHANNEL_ID = "adms_mobile_ad_observatory_notification_recording_channel";
   // The front-facing name associated with the periodic notification channel
   public static String NOTIFICATION_RECORDING_CHANNEL_ID_NAME = "Recording Reminders";
   // The front-facing description associated with the periodic notification channel
   public static String NOTIFICATION_RECORDING_CHANNEL_DESCRIPTION = "Be informed of when our app has started collecting Facebook ads";

   /*
    *
    * This method retrieves the title of the notification that is sent off whenever the app starts recording
    *
    * */
   public static String get_NOTIFICATION_RECORDING_TITLE(Context context) {
      return "The "+getApplicationName(context)+" has started recording ads";
   }

   public static String NOTIFICATION_RECORDING_DESCRIPTION =
         "The app is now collecting Facebook ads that have been served to you";

   /*
   *
   * This method retrieves persistent shared preference values
   *
   * */
   public static String sharedPreferenceGet(Context context, String name, String defaultValue) {
      SharedPreferences preferences = context.getSharedPreferences(getApplicationName(context), Context.MODE_PRIVATE);
      return preferences.getString(name, defaultValue);
   }

   /*
    *
    * This method assigns persistent shared preference values
    *
    * */
   public static void sharedPreferencePut(Context context, String name, String value) {
      SharedPreferences preferences = context.getSharedPreferences(getApplicationName(context), Context.MODE_PRIVATE);
      SharedPreferences.Editor editor = preferences.edit();
      editor.putString(name, value);
      editor.apply();
   }

   // if its instantiated, get the object associated with it, whereas if not, load in the value from shared preferences
}