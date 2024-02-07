/*
*
* This class is responsible for handling OCR functionality
*
* */

package com.adms.australianmobileadobservatory;

import static org.bytedeco.javacpp.lept.pixDestroy;
import static org.bytedeco.javacpp.lept.pixRead;
import android.content.Context;
import android.graphics.Bitmap;
import android.util.Log;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.lept;
import org.bytedeco.javacpp.tesseract;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Random;

public class OCRManager {
    private String TAG = "OCRManager";
    // The main directory of the app
    private File mainDir;
    // Whether or not OCR has been initialised (for memory consumption)
    private boolean ocrHasInitialized;
    // The Google Tesseract API container
    private static tesseract.TessBaseAPI api;

    /*
    *
    * The OCRManager class initiates the Google Tesseract API for OCR functionality
    *
    * */
    OCRManager(Context context){
        // If the OCR functionality hasn't initialized
        if (!ocrHasInitialized) {
            // Get the main directory
            mainDir = MainActivity.getMainDir(context);
            api = new tesseract.TessBaseAPI();
            // Initialize the OCR functionality of 'English' language
            if (api.Init(mainDir + (File.separatorChar + "tessdata"), "eng") != 0) {
                ocrHasInitialized = false;
                Log.e(TAG, "Failed on OCRManager: Couldn't initialize Google Tesseract API");
            }else {
                ocrHasInitialized = true;
            }
        }
    }

    /*
    *
    * This method searches an image for a string, using the OCR functionality
    *
    * */
    public boolean searchForString(List<String> searchForArray, Bitmap searchIn){
        // Open the image
        Bitmap searchInUpscaled = Bitmap.createScaledBitmap(
              searchIn,(int)Math.floor(searchIn.getWidth()
                    * Settings.recordScaleDivisor*Settings.recordScaleDivisorOCRUpscale),
              (int)Math.floor(searchIn.getHeight()
                    * Settings.recordScaleDivisor*Settings.recordScaleDivisorOCRUpscale),false);
        searchInUpscaled = Bitmap.createBitmap(
              searchInUpscaled, 0, 0,
              (int)Math.round(searchInUpscaled.getWidth()*Settings.OCRImageCropFromLeft),
              searchInUpscaled.getHeight());
        lept.PIX image = convertBMPToPix(searchInUpscaled);
        // Apply the image to the OCR functionality
        api.SetImage(image);
        // Get the result
        BytePointer outText;
        outText = api.GetUTF8Text();
        String foundText = outText.getString();
        Log.i(TAG, "\t\t* Text in image: "+foundText);
        // Determine if the string is in the image (ignoring case)
        for (String searchFor : searchForArray) {
            if (containsIgnoreCase(foundText, searchFor)) {
                Log.i(TAG, "\t\t* Found text '"+searchFor+"' in image");
                outText.deallocate();
                pixDestroy(image);
                return true;
            }
        }
        // Clear the memory that was used
        // Return the result
        outText.deallocate();
        pixDestroy(image);
        return false;
    }

    /*
    *
    * This method determines if a string contains a substring, ignoring case (and handling a few
    * tedious aspects in the process)
    *
    * */
    public static boolean containsIgnoreCase(String str, String searchStr) {
        // If either string is false, short-circuit to false
        if (str == null || searchStr == null) return false;
        // If the searchStr variable is of length 0, short-circuit to true
        final int length = searchStr.length();
        if (length == 0) return true;
        // Run the search
        for (int i = str.length() - length; i >= 0; i--) {
            if (str.regionMatches(true, i, searchStr, 0, length))
                return true;
        }
        // If nothing was found, return null
        return false;
    }

    /*
    *
    * This method safely closes the Google Tesseract API, releasing memory in the process
    *
    * */
    public static void closeOCR(){
        api.End();
    }

    /*
    *
    * This method attempts to convert a Bitmap to a Leptonica PIX file, for use with OCR
    * functionality
    *
    * */
    private lept.PIX convertBMPToPix(Bitmap img) {
        // The amount to compress the image during conversion
        int imageCompression = Settings.IMAGE_CONVERSION_QUALITY;
        // Initialise the image as null (no guarantee this method will succeed)
        lept.PIX pixImage = null;
        try {
            // Apply the timestamp and a random sequence to generate a unique filename
            String thisFileName = (Long.toString(System.currentTimeMillis())
                                + ((new Random()).nextInt(80) + 65));
            // Generate the output file
            File file = new File(mainDir
                                + (File.separatorChar + "temp" + File.separatorChar));
            if ((!file.exists()) && (!file.mkdirs())) {
                Log.e(TAG, "Failed on convertBMPtoPIX: couldn't create a temporary file");
            }
            File outFile = new File(file, thisFileName);
            // Insert the contents into the output file through a stream
            FileOutputStream out = new FileOutputStream(outFile);
            img.compress(Bitmap.CompressFormat.JPEG, imageCompression, out);
            out.flush();
            out.close();
            pixImage = pixRead(outFile.getAbsolutePath());
            if ((outFile.exists()) && (!outFile.delete())) {
                Log.e(TAG, "Failed on convertBMPtoPIX: couldn't delete a temporary file");
            }
        } catch (Exception e) {
            Log.e(TAG, "Failed on convertBMPtoPIX: ", e);
        }
        return pixImage;
    }
}
