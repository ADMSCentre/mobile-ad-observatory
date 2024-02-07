/*
*
* This class deals with the necessary unit tests for the LogoDetector class, and is configured to
* run on a local machine, as opposed to an Android device
*
* */

package com.adms.australianmobileadobservatory;

import static com.adms.australianmobileadobservatory.LogoDetector.drawableToBitmap;
import static com.adms.australianmobileadobservatory.LogoDetector.logoDetectionOnFacebookNewsFeedInstance;
import static com.adms.australianmobileadobservatory.Settings.deviceConfigurationDetails;
import static org.junit.Assert.assertEquals;
import android.content.Context;
import android.graphics.Bitmap;
import android.util.Log;
import androidx.test.platform.app.InstrumentationRegistry;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;

import java.util.Objects;

@RunWith(RobolectricTestRunner.class)
public class LogoDetectorUnitTest {
    private static final String TAG = "LogoDetectorUnitTests";
    // The suffixes that correspond to the various sub-images that may commmunicate a Facebook News
    // Feed logo pictogram
    private final String[] logoDetectionTestSuffixes = Settings.logoDetectionTestSuffixes;
    // Initialise a test context in which the unit tests will be undertaken
    public final Context testContext = InstrumentationRegistry.getInstrumentation().getTargetContext();

    /*
    *
    * This method provides statistical information for N executions of the logo detection
    * functionality, across all sub-image variations, for the Pixel 7 Pro configuration. Ideally,
    * we want to minimise the time it takes for it run
    *
    * */
    @Test
    public void logoDetectionPressureTestExecuteNTimes() throws JSONException {
        Log.i(TAG, "Beginning unit test logoDetectionPressureTestExecuteNTimes");
        // The number of executions to run (multiply by number of sub-image variations to get
        // exact number of individual tests
        int numberOfExecutions = Settings.numberOfExecutions;
        // The device configuration details (for all devices)
        JSONObject thisDeviceConfigurationDetails = deviceConfigurationDetails();
        long startTime = System.nanoTime();
        // Run the executions
        for (int j = 0; j < numberOfExecutions; j ++) {
            Log.i(TAG, "Beginning unit test "+j);
            // Isolate the configuration detail for the Pixel 7 Pro device
            JSONObject pixel7ProConfigurationDetail = (
                  (JSONObject) thisDeviceConfigurationDetails.get("pixel_7_pro"));
            // For each of the sub-image variations
            for (String suffix : logoDetectionTestSuffixes) {
                // Run the logo detection functionality from within the testContext
                logoDetectionOnFacebookNewsFeedInstance(
                      pixel7ProConfigurationDetail,
                      drawableToBitmap(drawableIdentifier(
                            pixel7ProConfigurationDetail, suffix), testContext)
                );
            }
        }
        Log.i(TAG, "Ending unit test logoDetectionPressureTestExecuteNTimes: "
                                + ((System.nanoTime() - startTime) / 1e+9));
    }

    /*
    *
    * This method tests a logo detection for configurationDetail on a single sub-image variation
    *
    * */
    public int logoDetectionTestTemplateInstance(JSONObject configurationDetail, Bitmap bitmapToTest) {
        try {

            return logoDetectionOnFacebookNewsFeedInstance(configurationDetail, bitmapToTest).size();
        } catch (Exception e) {
            Log.e(TAG, "Failure in logoDetectionTestTemplateInstance: ", e);
            return 0;
        }
    }

    /*
    *
    * This method returns a drawable identifier from the supplied configurationDetail and suffix
    *
    * */
    public int drawableIdentifier(JSONObject thisConfigurationDetail, String suffix) {
        try {
            return testContext.getResources().getIdentifier(
              thisConfigurationDetail.get("identifier") + "_" + suffix, "drawable",
              testContext.getPackageName());

        } catch (Exception e) {
            return 0;
        }
    }

    /*
    *
    * This method provides the general template for logo detection unit test functionality, by running
    * the functionality on all sub-image variations, and enforcing the necessary assertions
    *
    * */
    public void logoDetectionTestTemplate(JSONObject thisConfigurationDetail) {
        // For each of the logo detection suffixes
        for (String suffix : logoDetectionTestSuffixes) {
            // Negative matches should be asserted as false
            int evaluateMatchSize = ( suffix.endsWith("_negative") ? 0 : 1);
            assertEquals(evaluateMatchSize, logoDetectionTestTemplateInstance(
                  thisConfigurationDetail, drawableToBitmap(
                        drawableIdentifier(thisConfigurationDetail, suffix),
                        testContext)
                  )
            );
        }
    }

    /*
    *
    * This method implements the unit test for all logo detection configurations
    *
    * */
    @Test
    public void logoDetectionUnitTest() throws JSONException {
        JSONObject thisDeviceConfigurationDetails = deviceConfigurationDetails();
        JSONArray thisNames = Objects.requireNonNull(thisDeviceConfigurationDetails.names());
        for (int i = 0; i < thisNames.length(); i++) {
            logoDetectionTestTemplate(
                  (JSONObject)thisDeviceConfigurationDetails.get(
                        thisNames.getString(i)
                  )
            );
        }
    }

    public void logoDetectionSystemVersion() {
        String s="Debug-infos:";
        s += "\n OS Version: " + System.getProperty("os.version") + "(" + android.os.Build.VERSION.INCREMENTAL + ")";
        s += "\n OS API Level: " + android.os.Build.VERSION.SDK_INT;
        s += "\n Device: " + android.os.Build.DEVICE;
        s += "\n Model (and Product): " + android.os.Build.MODEL + " ("+ android.os.Build.PRODUCT + ")";
        System.out.println(s);
    }
}