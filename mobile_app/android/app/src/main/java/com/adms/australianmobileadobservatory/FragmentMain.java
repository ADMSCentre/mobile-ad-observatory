package com.adms.australianmobileadobservatory;

import static com.adms.australianmobileadobservatory.MainActivity.PERMISSION_CODE;
import static com.adms.australianmobileadobservatory.MainActivity.mProjectionManager;
import static com.adms.australianmobileadobservatory.Settings.SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE;
import static com.adms.australianmobileadobservatory.Settings.sharedPreferenceGet;

import android.content.Intent;
import android.os.Bundle;
import android.text.method.LinkMovementMethod;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Switch;
import android.widget.TextView;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentTransaction;

import java.util.Objects;

public class FragmentMain extends Fragment {
   // The tag of this class
   private static final String TAG = "FragmentMain";
   // The SwitchCompat toggler used with screen-recording
   private static Switch mToggleButton;
   // TODO - comment
   private Button mregisterButton;
   // De-bouncer variable on toggler
   private static boolean mToggleButtonDebouncerActivated;
   // The registration status of the user
   public static boolean THIS_REGISTRATION_STATUS = false;

   @Override
   public View onCreateView(LayoutInflater inflater, ViewGroup container,
                            Bundle savedInstanceState) {
      THIS_REGISTRATION_STATUS = (!Objects.equals(sharedPreferenceGet(
            getActivity(), "SHARED_PREFERENCE_REGISTERED", SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE), SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE));
      // TODO Auto-generated method stub

      View view = inflater.inflate(R.layout.fragment_main, container, false);

      // Attach the variable mToggleButton to the control within the view
      mToggleButton = (Switch) view.findViewById(R.id.simpleSwitch);
      // TODO // - comment
      mregisterButton = (Button) view.findViewById(R.id.buttonRegister);
      // Initialise the de-bouncer on the mToggleButton control
      mToggleButtonDebouncerActivated = false;
      // Apply a listener to the mToggleButton control, to execute the onToggleScreenShare method
      mToggleButton.setOnClickListener(v -> {
         if (((Switch)view.findViewById(R.id.simpleSwitch)).isChecked()) {
            // ask for permission to capture screen and act on result after
            getActivity().startActivityForResult(mProjectionManager.createScreenCaptureIntent(), PERMISSION_CODE);
            Log.v(TAG, "Screen-recording has started");
         } else {
            Log.v(TAG, "Screen-recording has stopped");
            Intent intent = new Intent(getActivity(), RecordService.class);
            getActivity().stopService(intent);
         }
         mToggleButtonDebouncerActivated = true;
      });
      // TODO - comment
      mregisterButton.setOnClickListener(v ->{
         Fragment fragment = new FragmentRegistration1();

         FragmentTransaction transaction = getParentFragmentManager().beginTransaction();
         transaction.setCustomAnimations(
                     R.anim.enter_from_right,  // enter
                     R.anim.exit_to_left,  // exit
                     R.anim.enter_from_left,   // popEnter
                     R.anim.exit_to_right  // popExit
               );
         transaction.replace(R.id.fragmentContainerView, fragment);
         transaction.addToBackStack(null);
         transaction.commit();
         //startActivity(new Intent(this, RegistrationActivity.class));
      });

      ((TextView)view.findViewById(R.id.fragment_main_learn_more)).setMovementMethod(LinkMovementMethod.getInstance());
      ((TextView)view.findViewById(R.id.fragment_main_privacy_policy)).setMovementMethod(LinkMovementMethod.getInstance());
      ((TextView)view.findViewById(R.id.fragment_main_learn_more_unregistered)).setMovementMethod(LinkMovementMethod.getInstance());
      ((TextView)view.findViewById(R.id.fragment_main_privacy_policy_unregistered)).setMovementMethod(LinkMovementMethod.getInstance());

      // If the device is registered
      if (THIS_REGISTRATION_STATUS) { // NB: This can be inverted for testing purposes - default is (!THIS_REGISTRATION_STATUS)
         // Hide the 'unregistered' screen
         view.findViewById(R.id.fragment_main_unregistered).setVisibility(View.GONE);

         Button buttonDashboard = (Button) view.findViewById(R.id.buttonDashboard);
         buttonDashboard.setOnClickListener(v ->{
            Fragment fragment = new FragmentDashboard();

            FragmentTransaction transaction = getParentFragmentManager().beginTransaction();
            transaction.setCustomAnimations(
                  R.anim.enter_from_left,  // enter
                  R.anim.exit_to_right,  // exit
                  R.anim.enter_from_right,   // popEnter
                  R.anim.exit_to_left  // popExit
            );
            transaction.replace(R.id.fragmentContainerView, fragment);
            transaction.addToBackStack(null);
            transaction.commit();
            //startActivity(new Intent(this, RegistrationActivity.class));
         });

      } else {
         // Or else hide the 'registered' screen
         view.findViewById(R.id.fragment_main_registered).setVisibility(View.GONE);
      }
      return view;


   }

   public static void setToggle(Boolean check) {
      try {
         if (!mToggleButtonDebouncerActivated) {
            mToggleButton.setChecked(check);
         }
      } catch (Exception e) {

      }

      // Then deactivate the de-bouncer in case (as we are resuming the app)
      mToggleButtonDebouncerActivated = false;
   }
}