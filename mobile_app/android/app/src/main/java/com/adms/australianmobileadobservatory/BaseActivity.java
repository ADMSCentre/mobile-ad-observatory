package com.adms.australianmobileadobservatory;

import android.app.Activity;
import android.content.Intent;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;

public class BaseActivity extends AppCompatActivity {

   @Override
   public void finish() {
      super.finish();
      overridePendingTransitionExit();
   }

   @Override
   public void startActivity(Intent intent) {
      super.startActivity(intent);
      overridePendingTransitionEnter();
   }
   /**
    * Overrides the pending Activity transition by performing the "Enter" animation.
    */
   protected void overridePendingTransitionEnter() {
      overridePendingTransition(R.anim.slide_from_right, R.anim.slide_to_left);
   }

   /**
    * Overrides the pending Activity transition by performing the "Exit" animation.
    */
   protected void overridePendingTransitionExit() {
      overridePendingTransition(R.anim.slide_from_left, R.anim.slide_to_right);
   }

   private Toolbar mActionBarToolbar;

   protected Toolbar getActionBarToolbar() {
      if (mActionBarToolbar == null) {
         mActionBarToolbar = (Toolbar) findViewById(R.id.toolbar_actionbar);
         if (mActionBarToolbar != null) {
            setSupportActionBar(mActionBarToolbar);
         }
      }
      return mActionBarToolbar;
   }

   @Override
   public void setContentView(int layoutResID) {
      super.setContentView(layoutResID);
      getActionBarToolbar();
   }

}