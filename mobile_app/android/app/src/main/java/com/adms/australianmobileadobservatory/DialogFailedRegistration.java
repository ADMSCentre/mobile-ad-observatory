package com.adms.australianmobileadobservatory;

import android.app.Dialog;
import android.content.Context;
import android.os.Bundle;
import android.view.View;
import android.view.Window;
import android.widget.Button;

import androidx.annotation.NonNull;
public class DialogFailedRegistration extends Dialog implements android.view.View.OnClickListener {

   private String TAG = "DialogFailedRegistration";

   public DialogFailedRegistration(@NonNull Context context) {
      super(context);
   }

   @Override
   protected void onCreate(Bundle savedInstanceState) {
      super.onCreate(savedInstanceState);
      requestWindowFeature(Window.FEATURE_NO_TITLE);
      setContentView(R.layout.dialog_registration_error);

      Button mbuttonBackFromFailedRegistration = (Button)findViewById(R.id.buttonBackFromFailedRegistration);
      mbuttonBackFromFailedRegistration.setOnClickListener(this);

      setCancelable(false);
      setCanceledOnTouchOutside(false);
   }

   @Override
   public void onClick(View view) {
      if (R.id.buttonBackFromFailedRegistration == view.getId()) {
         dismiss();
      }
   }
}