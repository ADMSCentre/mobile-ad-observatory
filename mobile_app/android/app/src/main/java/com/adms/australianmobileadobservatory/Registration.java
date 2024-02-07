package com.adms.australianmobileadobservatory;

import static com.adms.australianmobileadobservatory.LogManager.httpRequestRegistration;

import android.os.AsyncTask;

import org.json.JSONObject;

public class Registration extends AsyncTask<Object, Void, Boolean> {
   public AsyncResponse delegate = null;
   @Override
   protected Boolean doInBackground(Object... objects) {
      return httpRequestRegistration((JSONObject)objects[0]);
   }
   @Override
   protected void onPostExecute(Boolean result) {
      super.onPostExecute(result);
      delegate.processFinish(result);
   }
}
