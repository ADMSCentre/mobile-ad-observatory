/*
*
* This class deals with video operations, for reading bitmap data from MP4 files.
*
* */

package com.adms.australianmobileadobservatory;

import android.util.Log;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;

public class VideoManager {
    private static FrameGrabber videoGrabber;
    private static final String TAG = "VideoManager";

    /*
     *
     * This method attempts to initialise a VideoManager instance
     *
     * */
    VideoManager(String videoFilePath) {
        // Attempt to initialise an instance of the VideoManager class, of format MP4
        try {
            videoGrabber = new FFmpegFrameGrabber(videoFilePath);
            videoGrabber.setFormat("mp4");
            videoGrabber.start();
        } catch (FrameGrabber.Exception e) {
            Log.e(TAG, "Failed to initialise videoGrabber:", e);
        }
    }

    /*
     *
     * This method attempts to grab the next frame of a video file from within the videoGrabber
     *
     * */
    public Frame getNextFrame() {
        // Attempt to grab frame from videoGrabber, or report an error otherwise
        Frame vFrame = null;
        try {
            vFrame = videoGrabber.grabFrame();
        } catch (FrameGrabber.Exception e) {
            Log.e(TAG, "Failed to grab frame from videoGrabber:", e);
        }
        // Also, if the process fails, stop videoGrabber
        if( vFrame == null ) {
            stop();
        }
        return vFrame;
    }

    /*
    *
    * This method attempts to stop the videoGrabber
    *
    * */
    public void stop() {
        // Attempt to stop videoGrabber, or report an error otherwise
        try {
            videoGrabber.stop();
        } catch (FrameGrabber.Exception e) {
            Log.e(TAG, "Failed to stop videoGrabber:", e);
        }
    }
}
