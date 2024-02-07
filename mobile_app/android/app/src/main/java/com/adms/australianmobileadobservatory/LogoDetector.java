/*
*
* This class deals with the logic that fundamentally undertakes the logo detection.
*
* How does the weak logo detector work?
*
* Short description: The weak logo detector finds an instance of the Facebook "Home" button
* within the individual's News Feed.
*
* Long description: The weak logo detector accepts a still frame of a screen-logger capture.
* It processes the frame by visually analysing the image contents within the frame, to then
* determine whether the pictogram corresponding to the 'active' Facebook News Feed is
* present within said frame.
*
* Presently, the area of visual analysis is a statically-defined top-left section of the
* frame. In future, processing speeds will determine whether this changes to a more
* 'isolated' and dynamically defined area.
*
* When searching for the pictogram, the area of visual analysis is stridden by the bounds of
* a 'detector', which samples a small area of pixels to approximately determine that the
* Facebook pictogram is present.
*
* The 'detector' auto-adjusts to match numerous different scalations of the pictogram, to
* anticipate the different aspect ratios that may be encountered.
*
* Furthermore, the 'detector' is agnostic to the color of the pictogram, as different
* device settings may re-color the pictogram to suit 'night/day' themes.
*
* */

package com.adms.australianmobileadobservatory;

import static com.adms.australianmobileadobservatory.MainActivity.thisDeviceIdentifier;
import static com.adms.australianmobileadobservatory.Settings.deviceConfigurationDetails;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Color;
import android.graphics.drawable.BitmapDrawable;
import android.util.Log;
import androidx.core.content.ContextCompat;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class LogoDetector {
    private static final String TAG = "logoDetector";
    // The maximum acceptable intersection (as a percentage of overlap) of two identified logos
    public static double MAX_INTERSECTION_PERCENTAGE = Settings.LOGO_MAX_INTERSECTION_PERCENTAGE;
    // The first boolean matrix represents the pictogram of the Facebook News Feed icon, where every
    // 'true' value is a pixel that is colored into the pictogram, and every false value is
    // whitespace - the second corresponds to the 'notification' bubble that occasionally appears
    // aside it
    public static boolean[][] BOOLEAN_PICTOGRAM_MATRIX =
                                                Settings.LOGO_BOOLEAN_PICTOGRAM_MATRIX;
    public static boolean[][] BOOLEAN_PICTOGRAM_NOTIFICATION_MATRIX =
                                                Settings.LOGO_BOOLEAN_PICTOGRAM_NOTIFICATION_MATRIX;

    /*
    *
    * This method calculates the mean of a list of numerical elements
    *
    * */
    public static double calculateMean(List<Double> data) {
        int sum = 0;
        for (double d : data) sum += d;
        return sum / (double)data.size();
    }

    /*
    *
    * This method calculates the standard deviation of a list of numerical elements
    *
    * */
    public static double calculateStandardDeviation(List<Double> array) {
        double mean = calculateMean(array);
        double standardDeviation = 0.0;
        for (double num : array) {
            standardDeviation += Math.pow(num - mean, 2);
        }
        return Math.sqrt(standardDeviation / array.size());
    }

    /*
    *
    * This method returns the color type RGB colour for any of the three possible options (red,
    * green, or blue)
    *
    * */
    public static int colorType(int color, int type) {
        switch (type) {
            case 0 : return Color.red(color);
            case 1 : return Color.blue(color);
            case 2 : return Color.green(color);
        }
        return 0;
    }

    /*
     *
     * This method determines the average of the standard deviations of the color-types of a list of
     * readings. The result is a good indication of how inconsistently all the entries within the
     * readings list adhere to one color identity
     *
     * */
    public static int readingsInconsistency(List<Integer> readings) {
        List<List<Double>> readingsColorTypes = new ArrayList<>();
        List<Double> distances = new ArrayList<>();
        // For each color-type
        for (int i=0; i<3; i++) {
            // Initialise the designated array
            readingsColorTypes.add(new ArrayList<>());
            // For each of the readings, fill the designated array with the values of the
            // current color-type
            for(int j = 0; j < readings.size(); j++) {
                readingsColorTypes.get(i).add((double)colorType(readings.get(j), i));
            }
            // Calculate the standard deviation of the readings
            distances.add(calculateStandardDeviation(readingsColorTypes.get(i)));
        }
        // Return the mean of the standard deviations of the readings' color-types
        return (int)calculateMean(distances);
    }

    /*
     *
     * This method calculates the distance of two colors, as a measure of the summed distance of
     * all color-types that form the identities of both colors
     *
     * */
    public static int colorDistance(int colorA, int colorB) {
        int summatedDistance = 0;
        // For each of the color-types
        for (int i=0; i<3; i++) {
            // Retrieve the distance between both colors
            summatedDistance += Math.abs(colorType(colorA, i) - colorType(colorB, i));
        }
        return summatedDistance;
    }

    /*
     *
     * This method determines the average color from a list of readings
     *
     * */
    public static int determineAverageColour(List<Integer> readings) {
        List<List<Double>> readingsColorTypes = new ArrayList<>();
        int[] averageColorTypes = new int[3];
        // For each color-type
        for (int i=0; i<3; i++) {
            // Initialise the designated array
            readingsColorTypes.add(new ArrayList<>());
            // For each of the readings, fill the designated array with the values of the
            // current color-type
            for(int j = 0; j < readings.size(); j++) {
                readingsColorTypes.get(i).add((double)colorType(readings.get(j), i));
            }
            // Average the readings for the color-type
            averageColorTypes[i] = (int)calculateMean(readingsColorTypes.get(i));
        }
        // Return the constructed color
        return Color.rgb(averageColorTypes[0], averageColorTypes[1], averageColorTypes[2]);
    }

    /*
     *
     * This method determines the intersection percentage of two rectangles
     *
     * */
    public static double intersectionOfTwoRectangles(int[] rectangleA, int[] rectangleB) {
        // Method derived from
        // https://stackoverflow.com/questions/9324339/how-much-do-two-rectangles-overlap
        int XA1 = rectangleA[0];
        int YA1 = rectangleA[1];
        int XA2 = rectangleA[0] + rectangleA[2];
        int YA2 = rectangleA[1] + rectangleA[3];
        int XB1 = rectangleB[0];
        int YB1 = rectangleB[1];
        int XB2 = rectangleB[0] + rectangleB[2];
        int YB2 = rectangleB[1] + rectangleB[3];
        int SI = (Math.max(0, Math.min(XA2, XB2) - Math.max(XA1, XB1))
                * Math.max(0, Math.min(YA2, YB2) - Math.max(YA1, YB1)));
        return (double)SI / (((XA1-XA2) * (YA1 - YA2)) + ((XB1-XB2) * (YB1 - YB2)) - SI);
    }

    /*
    *
    * This method gets the device identifier for the device that is running the app, as is necessary
    * for specialised logo detection functionality
    *
    * */
    public static String getDeviceIdentifier(String thisBuildModel) {
        try {
            // Attempt to iterate over the deviceConfigurationDetails - if a configuration detail
            // is found that has the same device build model as thisBuildModel, return its configuration
            // detail
            JSONObject thisDeviceConfigurationDetails = deviceConfigurationDetails();
            Iterator<String> keys = thisDeviceConfigurationDetails.keys();
            while(keys.hasNext()) {
                String key = keys.next();
                if (thisDeviceConfigurationDetails.get(key) instanceof JSONObject) {
                    if (((JSONObject) thisDeviceConfigurationDetails.get(key)).get("deviceBuildModel").equals(thisBuildModel)) {
                        return (String) ((JSONObject) thisDeviceConfigurationDetails.get(key)).get("identifier");
                    }
                }
            }
            return null;
        } catch (Exception e) {
            Log.e(TAG, "Failed on getDeviceIdentifier: ", e);
            return null; // TODO - general identifier
        }
    }



    /*
     *
     * This method converts a drawable object into a bitmap
     *
     * */
    public static Bitmap drawableToBitmap(int thisDrawable, Context testContext) {
        return ((BitmapDrawable) Objects.requireNonNull(
              ContextCompat.getDrawable(testContext, thisDrawable))).getBitmap();
    }

    /*
     *
     * This function determines whether the Facebook 'Home' pictogram appears within a supplied
     * image, given a configuration detail.
     *
     * */
    public static List<JSONObject> logoDetectionOnFacebookNewsFeedInstance(
            JSONObject thisConfigurationDetail,
            Bitmap testBitmapUnscaled) throws JSONException {
        // Load in the downscale
        double downScale = (double)thisConfigurationDetail.get("downScale");
        // Downscale the test bitmap
        Bitmap testBitmap = Bitmap.createScaledBitmap(
              testBitmapUnscaled,(int)Math.floor(testBitmapUnscaled.getWidth()*downScale),
              (int)Math.floor(testBitmapUnscaled.getHeight()*downScale),false);
        // Create a list that will contain the logo matches discovered during analysis
        List<JSONObject> matches = new ArrayList<>();
        // Specify the ratios of the anticipated viewport dimensions, against those of the actual
        // specification.
        double ratioW = ((double)testBitmapUnscaled.getWidth()
                / ((double)thisConfigurationDetail.get("anticipatedViewportW")));
        double ratioH = ((double)testBitmapUnscaled.getHeight()
                / ((double)thisConfigurationDetail.get("anticipatedViewportH")));
        // Define the area to visually analyse, by means of the provided ratios
        int adjustedOriginX = (int)((
                Math.floor((int)thisConfigurationDetail.get("anticipatedOriginX") * ratioW)
                        - Math.floor((int)thisConfigurationDetail.get("jitter") * ratioW)) * downScale);
        int adjustedOriginY = (int)((
                Math.floor((int)thisConfigurationDetail.get("anticipatedOriginY") * ratioH)
                        - Math.floor((int)thisConfigurationDetail.get("jitter") * ratioH)) * downScale);
        int adjustedOriginYOffset = (int)((
                Math.floor((int)thisConfigurationDetail.get("anticipatedOriginYOffset") * ratioH)
                        - Math.floor((int)thisConfigurationDetail.get("jitter") * ratioH)) * downScale);
        int adjustedOriginXOffset = (int)((
                Math.floor((int)thisConfigurationDetail.get("anticipatedOriginXOffset") * ratioW)
                        - Math.floor((int)thisConfigurationDetail.get("jitter") * ratioW)) * downScale);
        int adjustedLogoDiameterW = (int)Math.floor(
                (int)thisConfigurationDetail.get("anticipatedLogoDiameter") * ratioW * downScale);
        int adjustedLogoDiameterH = (int)Math.floor(
                (int)thisConfigurationDetail.get("anticipatedLogoDiameter") * ratioH * downScale);
        // Apply jitters (for possible mis-alignment)
        Bitmap areaToVisuallyAnalyse = Bitmap.createBitmap(
                testBitmap,
                adjustedOriginX,
                adjustedOriginY,
                adjustedLogoDiameterW+(int)(
                        (int)thisConfigurationDetail.get("jitter") * ratioW * 2 * downScale),
                adjustedLogoDiameterH+(int)(
                        (int)thisConfigurationDetail.get("jitter") * ratioH * 2 * downScale));
        // The offset is also defined (for when the navbar switches position)
        Bitmap areaToVisuallyAnalyseOffset = Bitmap.createBitmap(
                testBitmap,
                adjustedOriginXOffset,
                adjustedOriginYOffset,
                adjustedLogoDiameterW+(int)(
                        (int)thisConfigurationDetail.get("jitter") * ratioW * 2 * downScale),
                adjustedLogoDiameterH+(int)(
                        (int)thisConfigurationDetail.get("jitter") * ratioH * 2 * downScale));
        /*
        // Uncomment for debugging
        Log.i(TAG,
              "adjustedOriginX: " + adjustedOriginX + " adjustedOriginY: " + adjustedOriginY
                + "\nw: " + ( adjustedLogoDiameterW +
                        (int)((int)thisConfigurationDetail.get("jitter") * ratioW * downScale))
                + "\nh: " + ( adjustedLogoDiameterH +
                        (int)((int)thisConfigurationDetail.get("jitter") * ratioH * downScale)));
        // Monitor the number of individual evaluations conducted when searching for the logo
        // within the bitmap
        int evaluationsN = 0;
        */
        List<Bitmap> areasToVisuallyAnalyse = new ArrayList<>();
        areasToVisuallyAnalyse.add(areaToVisuallyAnalyse);
        areasToVisuallyAnalyse.add(areaToVisuallyAnalyseOffset);
        // The stride is scaled
        int scaledStride = (int)Math.ceil((int)thisConfigurationDetail.get("stride") * downScale);
        // For the areas to visually analyse (whether by offset or not)
        for (int areaToAnalyse_i = 0; areaToAnalyse_i < areasToVisuallyAnalyse.size(); areaToAnalyse_i ++) {
            Bitmap thisAreaToVisuallyAnalyse = areasToVisuallyAnalyse.get(areaToAnalyse_i);
            // For the X and Y coordinates to stride
            for (int strideX = 0;
                 strideX+adjustedLogoDiameterW <= thisAreaToVisuallyAnalyse.getWidth();
                 strideX += scaledStride) {
                for (int strideY = 0;
                     strideY+adjustedLogoDiameterH <= thisAreaToVisuallyAnalyse.getHeight();
                     strideY += scaledStride) {
                    // Determine if the overlap of the proposed test and the existing matches is
                    // too significant to warrant an evaluation
                    boolean warrantsAnEvaluation = true;
                    for (int i = 0; i < matches.size(); i ++) {
                        if (intersectionOfTwoRectangles(
                                new int[]{
                                        (int) matches.get(i).get("strideX"),
                                        (int) matches.get(i).get("strideY"),
                                        (int) matches.get(i).get("logoDiameterW"),
                                        (int) matches.get(i).get("logoDiameterH") },
                                new int[]{
                                        strideX,
                                        strideY,
                                        adjustedLogoDiameterW,
                                        adjustedLogoDiameterH }) > MAX_INTERSECTION_PERCENTAGE) {
                            warrantsAnEvaluation = false;
                        }
                    }
                    // If the evaluation has been warranted
                    if (warrantsAnEvaluation) {
                        /*
                        // Uncomment for debugging
                        // Tally the evaluation
                        evaluationsN ++;
                        Log.i(TAG, "strideX: " + strideX + " strideY: " + strideY + "\n"
                                + " adjustedLogoDiameterW: " + adjustedLogoDiameterW
                                + " adjustedLogoDiameterH: " + adjustedLogoDiameterH + "\n"
                                + "thisAreaToVisuallyAnalyse.getWidth(): "
                                + thisAreaToVisuallyAnalyse.getWidth()
                                + " thisAreaToVisuallyAnalyse.getHeight(): "
                                + thisAreaToVisuallyAnalyse.getHeight());
                        */
                        // Construct the bitmap that corresponds to the area that will be analysed
                        Bitmap partOfAreaToVisuallyAnalyse = Bitmap.createBitmap(
                                thisAreaToVisuallyAnalyse, strideX, strideY,
                                adjustedLogoDiameterW, adjustedLogoDiameterH);
                        // From within the area to analyse, match each positioned pixel to the
                        // boolean pictogram matrix, to get an idea about which pixels correspond
                        // to 'positive' readings (i.e. those that are colored), and those that
                        // correspond to 'negative' readings (i.e. those that are uncolored)
                        List<Integer> positiveReadings = new ArrayList<>();
                        List<Integer> negativeReadings = new ArrayList<>();
                        for (int xPosition = 0;
                             xPosition < BOOLEAN_PICTOGRAM_MATRIX.length; xPosition++) {
                            for (int yPosition = 0;
                                 yPosition < BOOLEAN_PICTOGRAM_MATRIX.length; yPosition++) {
                                // Anticipate the existance of the notification bubble
                                if (!BOOLEAN_PICTOGRAM_NOTIFICATION_MATRIX[yPosition][xPosition]) {
                                    int xPixel = (int) Math.floor(((double) xPosition
                                            / BOOLEAN_PICTOGRAM_MATRIX.length)
                                            * partOfAreaToVisuallyAnalyse.getWidth() - 1);
                                    int yPixel = (int) Math.floor(((double) yPosition
                                            / BOOLEAN_PICTOGRAM_MATRIX.length)
                                            * partOfAreaToVisuallyAnalyse.getHeight() - 1);
                                    xPixel = Math.max(xPixel, 0);
                                    yPixel = Math.max(yPixel, 0);
                                    int pixel = partOfAreaToVisuallyAnalyse.getPixel(xPixel, yPixel);
                                    if (BOOLEAN_PICTOGRAM_MATRIX[xPosition][yPosition]) {
                                        positiveReadings.add(pixel);
                                    } else {
                                        negativeReadings.add(pixel);
                                    }
                                }
                            }
                        }
                        // Determine the distance between the average positive reading color and
                        // the average negative reading color
                        double colorDistance = colorDistance(
                                determineAverageColour(positiveReadings),
                                determineAverageColour(negativeReadings));
                        // Determine the inconsistencies among the positive and negative readings
                        int positiveReadingInconsistency = readingsInconsistency(positiveReadings);
                        int negativeReadingInconsistency = readingsInconsistency(negativeReadings);
                        // Uncomment for debugging
                        Log.i(TAG, "positiveReadingInconsistency: "+positiveReadingInconsistency
                                +" negativeReadingInconsistency: "+negativeReadingInconsistency
                                +" colorDistance: "+colorDistance);
                        // Use the aforementioned statistics to determine whether the pictogram is
                        // present, by differences in positive and negative readings, as well as
                        // the color contrast between the pictogram and the background
                        int readingMaxDifference = (int)thisConfigurationDetail
                                .get("readingMaxDifference");
                        int colorMaxDifference = (int)thisConfigurationDetail
                                .get("colorMaxDifference");
                        boolean positiveReadingsAreConsistency = (
                                Math.abs(positiveReadingInconsistency
                                        - (int)thisConfigurationDetail
                                        .get("positiveReadingConsistencyAnchor"))
                                        <= readingMaxDifference);
                        boolean negativeReadingsAreConsistency = (
                                Math.abs(negativeReadingInconsistency
                                        - (int)thisConfigurationDetail
                                        .get("negativeReadingConsistencyAnchor"))
                                        <= readingMaxDifference);
                        boolean colorDifferenceIsConsistent = (
                                Math.abs(colorDistance
                                        - (int)thisConfigurationDetail.get("colorDistanceAnchor"))
                                        <= colorMaxDifference);
                        // The above process is duplicated for dark modes as well (this cannot be
                        // anticipated, and is done in any case
                        boolean positiveReadingsAreConsistencyDark = (
                                Math.abs(positiveReadingInconsistency
                                        - (int)thisConfigurationDetail
                                        .get("positiveReadingConsistencyAnchorDark"))
                                        <= readingMaxDifference);
                        boolean negativeReadingsAreConsistencyDark = (
                                Math.abs(negativeReadingInconsistency
                                        - (int)thisConfigurationDetail
                                        .get("negativeReadingConsistencyAnchorDark"))
                                        <= readingMaxDifference);
                        boolean colorDifferenceIsConsistentDark = (
                                Math.abs(colorDistance
                                        - (int)thisConfigurationDetail.get("colorDistanceAnchorDark"))
                                        <= colorMaxDifference);
                        // If all conditions are fulfilled...
                        if ((positiveReadingsAreConsistency
                                && negativeReadingsAreConsistency
                                && colorDifferenceIsConsistent) ||
                                (positiveReadingsAreConsistencyDark
                                        && negativeReadingsAreConsistencyDark
                                        && colorDifferenceIsConsistentDark)) {
                            // Record the match
                            JSONObject match = new JSONObject();
                            match.put("identifier", thisConfigurationDetail.get("identifier"));
                            match.put("colorDistance", colorDistance);
                            match.put("strideX", strideX);
                            match.put("strideY", strideY);
                            match.put("logoDiameterW", adjustedLogoDiameterW);
                            match.put("logoDiameterH", adjustedLogoDiameterH);
                            match.put("positiveReadingInconsistency",
                                    positiveReadingInconsistency);
                            match.put("negativeReadingInconsistency",
                                    negativeReadingInconsistency);
                            matches.add(match);
                        }
                        /*
                        // Uncomment for debugging
                        // Write out the match
                        try (FileOutputStream out = new FileOutputStream("debug.png")) {
                            partOfAreaToVisuallyAnalyse.compress(
                                    Bitmap.CompressFormat.PNG, 100, out);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        */
                    }
                }
            }
        }
        /*
        // Print the matches
        Log.i(TAG, String.valueOf(evaluationsN));
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Log.i(TAG, gson.toJson(matches));
        */
        return matches;
    }

    /*
    *
    * This method returns the deviceConfigurationDetail for the current device
    *
    * */
    public static JSONObject thisDeviceConfigurationDetail() {
        // Attempt to load the device identifier for the specified device configuration
        JSONObject defaultConfigurationDetail = null;
        try {
            return (JSONObject)deviceConfigurationDetails().get(thisDeviceIdentifier);
        } catch (Exception e) {
            Log.e(TAG, "Failed on thisDeviceConfigurationDetail: ", e);
            return defaultConfigurationDetail;
        }
    }
}
