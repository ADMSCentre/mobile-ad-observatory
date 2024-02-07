/*
*
* This class deals with the direct analysis of video content (frame by frame), to identify instances
* where the individual is using their Facebook app, and then furthermore, if they are observing
* sponsored advertisement content. In such cases, the content is then submitted to an AWS Lambda
* endpoint
*
* */

package com.adms.australianmobileadobservatory;

import static com.adms.australianmobileadobservatory.LogoDetector.thisDeviceConfigurationDetail;
import static com.adms.australianmobileadobservatory.MainActivity.THIS_OBSERVER_ID;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Color;
import android.media.MediaExtractor;
import android.media.MediaFormat;
import android.util.Base64;
import android.util.Log;
import org.bytedeco.javacv.AndroidFrameConverter;
import org.bytedeco.javacv.Frame;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class LogManager {
    private static final String TAG = "LogManager";
    private final OCRManager ocrManager;
    private static File rootDirectoryPath;

    /*
    *
    * This method gets the image similarity of two images, by comparing their pixels, at a reduced
    * resolution.
    *
    * */
    private double getNaiveImageSimilarity(Bitmap lastBitmapFromFrame,
                                           Bitmap thisBitmapFromFrame) {
        // To up the accuracy of the method, increase this value //CONFIGURABLE
        int scalePixelsWidth = Settings.IMAGE_SIMILARITY_SCALE_PIXELS_WIDTH;
        // Derive the scaled height, by applying the ratio of the inserted bitmaps
        int scalePixelsHeight = Math.round( (float)lastBitmapFromFrame.getWidth()
                / (float)lastBitmapFromFrame.getHeight() * (float) scalePixelsWidth);
        // Generate the scaled bitmaps
        Bitmap lastBitmapFromFrameScaled = Bitmap.createScaledBitmap(
                lastBitmapFromFrame, scalePixelsWidth, scalePixelsHeight,false);
        Bitmap thisBitmapFromFrameScaled = Bitmap.createScaledBitmap(
                thisBitmapFromFrame, scalePixelsWidth, scalePixelsHeight,false);
        // Analyse the color difference (RGB) between the scaled bitmaps
        double maximumCumulativeDifference = (255.0 * 3.0 * scalePixelsWidth * scalePixelsHeight);
        long thisCumulativeDifference = 0;
        for (int y = scalePixelsHeight - 1; y >= 0; y--) {
            for (int x = scalePixelsWidth - 1; x >= 0; x--) {
                int c1 = lastBitmapFromFrameScaled.getPixel(x, y);
                int c2 = thisBitmapFromFrameScaled.getPixel(x, y);
                thisCumulativeDifference += (Math.abs(Color.red(c1) - Color.red(c2))
                        + Math.abs(Color.blue(c1) - Color.blue(c2))
                        + Math.abs(Color.green(c1) - Color.green(c2)));
            }
        }
        // Take the total cumulative difference, and divide it by the maximum possible difference
        return 1.0 - (thisCumulativeDifference / maximumCumulativeDifference);
    }

    /*
     *
     * This method initialises an instance of the LogManager class
     *
     * */
    LogManager(Context context){
        // The OCR Manager is applied here for the purpose of identifying the term "Sponsored"
        ocrManager = new OCRManager(context);
        // The rootDirectoryPath variable must be initialised here, to access the app context
        rootDirectoryPath = MainActivity.getMainDir(context);
    }

    /*
     *
     * This method initiates the startEventDetection for each of the processable videos
     *
     * */
    public void run() throws JSONException {
        try {
            // Retrieve the video files within the designated folder
            List<File> videoFiles = new ArrayList<>();
            // We add all to the empty array, to avoid unanticipated construction errors
            videoFiles.addAll(Arrays.asList(Objects.requireNonNull(new File(
                  rootDirectoryPath + (File.separatorChar
                        + "videos" + File.separatorChar)).listFiles())));
            // If there are at least two videos, then processing can begin - this is because
            // the LogManager can only spin up when the screen-recording functionality is
            // active or in the background during idle behaviour, which means that a 'WIP' video
            // will always be present when it activates - this does also introduce the issue that
            // the very last time the app closes, we will lose some data, however this should be
            // negligible
            int minimumNumberOfVideosForProcessing = 2;
            if (!videoFiles.isEmpty() && videoFiles.size() >= minimumNumberOfVideosForProcessing) {
                // The latest screen recording is identified
                int delete = 0;
                long latest = videoFiles.get(0).lastModified();
                for(int i = 0; i < videoFiles.size(); i++) {
                    if( videoFiles.get(i).lastModified() > latest){
                        latest = videoFiles.get(i).lastModified();
                        delete = i;
                    }
                }
                // It is then removed from the list of videos that will be processed (as it may
                // still be instantiating from the screen recording)
                videoFiles.remove(delete);
                // Loop through all the videoFiles, and run the startEventDetection for each
                int i = 0;
                while (i < videoFiles.size()) {
                    startEventDetection( videoFiles.get(i) );
                    i++;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "Failed to run LogManager: ", e);
        }

    }

    /*
    *
    * This method gets the exact frame rate of the video, as adapted from
    * https://stackoverflow.com/questions/42204944/how-to-get-frame-rate-of-video-in-android-os
    *
    * */
    private int getExactFrameRate(String filePath) {
        MediaExtractor extractor = new MediaExtractor();
        int frameRate = 24; //may be default
        try {
            extractor.setDataSource(filePath);
            int numTracks = extractor.getTrackCount();
            for (int i = 0; i < numTracks; ++i) {
                MediaFormat format = extractor.getTrackFormat(i);
                String mime = format.getString(MediaFormat.KEY_MIME);
                if (mime.startsWith("video/")) {
                    if (format.containsKey(MediaFormat.KEY_FRAME_RATE)) {
                        frameRate = format.getInteger(MediaFormat.KEY_FRAME_RATE);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            extractor.release();
        }
        return frameRate;
    }

    /*
    *
    * This method runs the logo detection functionality to determine if sponsored content is present
    * within the individual's Facebook News Feed, and submits instances that involve such content to
    * an AWS Lambda endpoint
    *
    * */
    // TODO introduce a faster looping function
    // This is achieved by upping the frame intervals (to perhaps a second, and then when
    // identifying Facebook, reduce the interval size, and step back one large interval to
    // preserve any possibly skipped frames
    // TODO Send up logoDetector statistics with regular data
    private void startEventDetection(File videoFilePath) {
        try {
            // The base number of frame intervals to skip
            int numberOfFrameIntervals = Settings.RECORDER_FRAME_INTERVALS;
            int maximumLastFrameWasAffirmative = Settings.RECORDER_FRAME_POSITIVE_COOLDOWN;
            double frameSimilarityThreshold = Settings.RECORDER_FRAME_SIMILARITY_THRESHOLD;
            // This event is timed for optimisation purposes
            long startTime = System.nanoTime();
            Log.i(TAG, "Beginning eventDetection of videoFilePath: " + videoFilePath);
            // Provided that the video is rotated to 'portrait' mode
            if(videoFilePath.getAbsolutePath().contains("portrait")) {
                // Load up the VideoManager instance for the video, and retrieve a frame
                VideoManager vManager = new VideoManager(videoFilePath.getAbsolutePath());
                int thisExactFrameRate = getExactFrameRate(videoFilePath.getAbsolutePath());
                Log.i(TAG, "thisExactFrameRate: " + thisExactFrameRate);
                Frame currentFrame = vManager.getNextFrame();
                // Cursor the frame with the ii variable, and 'cool-down' on frames that contain the
                // 'Sponsored' text by the lastFrameWasAffirmative variable
                int ii = 0;
                int lastFrameWasAffirmative = 0;
                Bitmap lastBitmapFromFrame = null;
                // Begin looping through the frames
                while (currentFrame != null) {
                    // Step through the frame, and synchronize the cursor
                    ii += 1;
                    currentFrame = vManager.getNextFrame();
                    // At every so many frames, implement an analysis
                    if (ii % numberOfFrameIntervals == 0) {
                        // Convert the frame into a bitmap
                        Bitmap thisBitmapFromFrame = (new AndroidFrameConverter()).convert(currentFrame);
                        boolean framesAreIdentical = false;
                        if (lastBitmapFromFrame != null) {
                            try {
                                // Determine the similarity of this frame to the last frame
                                double similarity = getNaiveImageSimilarity(
                                                        lastBitmapFromFrame,thisBitmapFromFrame);
                                Log.i(TAG, "\t* Similarity of frames " + ii
                                                        + " & " + (ii + 1) + " : " + similarity);
                                framesAreIdentical = (similarity > frameSimilarityThreshold);
                            } catch (Exception e) {
                                framesAreIdentical = true;
                            }
                        }
                        Log.i(TAG, "\t* Processing frame " + ii);
                        if (!framesAreIdentical) {
                            boolean isSponsored = false;
                            // If the frames aren't identical, and we haven't yet upped the cool-down,
                            // up it to maximumLastFrameWasAffirmative value (where "Sponsored" text
                            // is found
                            if (!(lastFrameWasAffirmative > 0)) {
                                // Apply the logoDetector to determine if the frame was taken from
                                // within Facebook
                                List<JSONObject> matches =
                                    LogoDetector.logoDetectionOnFacebookNewsFeedInstance(
                                          thisDeviceConfigurationDetail(),
                                          thisBitmapFromFrame
                                    );
                                // If the frame was taken from within Facebook, then there will be
                                // at least a single match
                                if ((matches.size() > 0)) {
                                    Log.i(TAG, "\t\t* Frame is taken from Facebook");
                                    // Then apply the ocrManager to determine if the frame contains
                                    // the word "Sponsored"
                                    Log.i(TAG, "\t\tOCR HAS STARTED");
                                    List<String> searchForArray = Arrays.asList(
                                          "Sponso",
                                          "ponsor",
                                          "onsore",
                                          "nsored",
                                          "Sponsor",
                                          "ponsore",
                                          "onsored",
                                          "Sponsored"
                                          );
                                    isSponsored = ocrManager.searchForString(
                                          searchForArray, thisBitmapFromFrame);
                                    Log.i(TAG, "\t\tOCR HAS FINISHED");
                                    // If it does, up the cooldown
                                    if (isSponsored) {
                                        lastFrameWasAffirmative = maximumLastFrameWasAffirmative;
                                    }
                                }
                            } else {
                                // In the case that nothing was identified, lower the cool-down,
                                // although retain the declaration that "Sponsored" text content
                                // was found
                                isSponsored = true;
                                lastFrameWasAffirmative --;
                            }
                            // If the frame has been identified as containing the "Sponsored" text
                            // content, submit the request to the server
                            if (isSponsored) {
                                httpRequestDataDonation(thisBitmapFromFrame,
                                      videoFilePath.getName(), String.valueOf(ii), thisExactFrameRate);
                                Log.i(TAG, "\t\t* Frame contains sponsored content");
                            }
                            if (lastFrameWasAffirmative > 0) {
                                Log.i(TAG, "\t\t* Frame is trailing sponsored content: "
                                                                        + lastFrameWasAffirmative);
                            }
                        } else {
                            Log.i(TAG, "\t* Bypassing due to identical frames");
                        }
                        // Set the last frame to the current frame before the next iteration
                        lastBitmapFromFrame = thisBitmapFromFrame;
                    }
                }
                Log.i(TAG, "\t* Time taken: " + ((System.nanoTime() - startTime) / 1e+9));
                Log.i(TAG, "Ending eventDetection of videoFilePath: " + videoFilePath);
            }
            // Inform us if the video cannot be deleted
            if ((videoFilePath.exists()) && (!videoFilePath.delete())) {
                Log.e(TAG, "Failed to delete videoFilePath: " + videoFilePath);
            }
        } catch (Exception e) {
            Log.e(TAG, "Failed to execute startEventDetection: ", e);
        }
    }

    /*
    *
    * This method attempts to send a HTTP POST request containing the screenshot data to the AWS
    * Lambda endpoint that is responsible for this project
    *
    * */
    private void httpRequestDataDonation(Bitmap thisBitmapFromFrame, String videoFileName,
                                         String currentFrame, int thisExactFrameRate) {
        try {
            // Declare the AWS Lambda endpoint
            String urlParam = Settings.AWS_LAMBDA_ENDPOINT;
            // The exported image quality
            int imageExportQuality = Settings.IMAGE_EXPORT_QUALITY;
            // The unique ID of the observer to insert with the HTTP request
            String observerID = THIS_OBSERVER_ID;
            // The identifier for submitting data donations
            String identifierDataDonation = Settings.IDENTIFIER_DATA_DONATION;
            // The HTTP request connection timeout (in milliseconds)
            int requestConnectTimeout = Settings.AWS_LAMBDA_ENDPOINT_CONNECTION_TIMEOUT;
            // The HTTP request read timeout (in milliseconds)
            int requestReadTimeout = Settings.AWS_LAMBDA_ENDPOINT_READ_TIMEOUT;
            // Write up the stream for inserting the image (as a Base64 string) into the request
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            thisBitmapFromFrame.compress(Bitmap.CompressFormat.JPEG, imageExportQuality, stream);
            String imageEncodedAsBase64 = Base64.encodeToString(
                                            stream.toByteArray(), Base64.DEFAULT);
            // Assemble the request JSON object
            JSONObject requestBody = new JSONObject();
            requestBody.put("action",identifierDataDonation);
            requestBody.put("observer_id",observerID);
            requestBody.put("video_filename",videoFileName);
            requestBody.put("current_frame",currentFrame);
            requestBody.put("exact_framerate",thisExactFrameRate);
            requestBody.put("imageEncodedAsBase64",imageEncodedAsBase64);
            String bodyParam = requestBody.toString();
            // Set up the HTTP request configuration
            URL url = new URL(urlParam);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", "text/plain");
            connection.setRequestProperty("Content-Type", "text/plain");
            connection.setConnectTimeout(requestConnectTimeout);
            connection.setReadTimeout(requestReadTimeout);
            OutputStream os = connection.getOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8);
            osw.write(bodyParam);
            osw.flush();
            osw.close();
            connection.connect();
            // Interpret the output
            BufferedReader rd = new BufferedReader(new InputStreamReader(
                                                            connection.getInputStream()));
            // TODO read output and determine shitty responses
        } catch (Exception e) {
            Log.e(TAG, "Failed to run httpRequestDataDonation: ", e);
        }
    }

    /*
     *
     * This method attempts to send a HTTP POST request containing the registration of the user
     * for data donations
     *
     * */
    public static boolean httpRequestRegistration(JSONObject registrationJSONObject) {
        try {
            // Declare the AWS Lambda endpoint
            String urlParam = Settings.AWS_LAMBDA_ENDPOINT;
            // The unique ID of the observer to insert with the HTTP request
            String observerID = THIS_OBSERVER_ID;
            // The identifier for submitting a registration
            String identifierDataDonation = Settings.IDENTIFIER_REGISTRATION;
            // The HTTP request connection timeout (in milliseconds)
            int requestConnectTimeout = Settings.AWS_LAMBDA_ENDPOINT_CONNECTION_TIMEOUT;
            // The HTTP request read timeout (in milliseconds)
            int requestReadTimeout = Settings.AWS_LAMBDA_ENDPOINT_READ_TIMEOUT;
            // Assemble the request JSON object
            JSONObject requestBody = new JSONObject();
            requestBody.put("action",identifierDataDonation);
            requestBody.put("observer_id",observerID);
            requestBody.put("user_details",registrationJSONObject);
            String bodyParam = requestBody.toString();
            // Set up the HTTP request configuration
            URL url = new URL(urlParam);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", "text/plain");
            connection.setRequestProperty("Content-Type", "text/plain");
            connection.setConnectTimeout(requestConnectTimeout);
            connection.setReadTimeout(requestReadTimeout);
            OutputStream os = connection.getOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8);
            osw.write(bodyParam);
            osw.flush();
            osw.close();
            connection.connect();
            // Interpret the output
            BufferedReader rd = new BufferedReader(new InputStreamReader(
                  connection.getInputStream()));
            Log.i(TAG, rd.readLine());
            // TODO read output and determine shitty responses
            return true;
        } catch (Exception e) {
            Log.e(TAG, "Failed to run httpRequestRegistration: ", e);
            return false;
        }
    }

}
