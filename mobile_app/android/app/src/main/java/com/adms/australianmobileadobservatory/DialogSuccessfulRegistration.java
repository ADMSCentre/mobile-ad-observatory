package com.adms.australianmobileadobservatory;

import android.app.Dialog;
import android.content.Context;
import android.os.Bundle;
import android.view.View;
import android.view.Window;
import android.widget.Button;

import androidx.annotation.NonNull;
public class DialogSuccessfulRegistration extends Dialog implements android.view.View.OnClickListener {

   private String TAG = "DialogSuccessfulRegistration";

   public DialogSuccessfulRegistration(@NonNull Context context) {
      super(context);
   }

   @Override
   protected void onCreate(Bundle savedInstanceState) {
      super.onCreate(savedInstanceState);
      requestWindowFeature(Window.FEATURE_NO_TITLE);
      setContentView(R.layout.dialog_registration_success);

      Button mbuttonBackFromCompleteRegistration = (Button)findViewById(R.id.buttonBackFromCompleteRegistration);
      mbuttonBackFromCompleteRegistration.setOnClickListener(this);

      setCancelable(false);
      setCanceledOnTouchOutside(false);
   }

   @Override
   public void onClick(View view) {
      if (R.id.buttonBackFromCompleteRegistration == view.getId()) {
         dismiss();
      }
   }
}