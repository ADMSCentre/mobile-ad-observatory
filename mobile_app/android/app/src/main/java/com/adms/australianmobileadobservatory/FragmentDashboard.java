package com.adms.australianmobileadobservatory;

import static com.adms.australianmobileadobservatory.Settings.SHARED_PREFERENCE_REGISTERED_DEFAULT_VALUE;
import static com.adms.australianmobileadobservatory.Settings.sharedPreferenceGet;

import android.os.Bundle;
import android.text.method.LinkMovementMethod;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentTransaction;

import java.util.Objects;

public class FragmentDashboard extends Fragment {

   @Override
   public View onCreateView(LayoutInflater inflater, ViewGroup container,
                            Bundle savedInstanceState) {


      View view = inflater.inflate(R.layout.fragment_dashboard, container, false);

      String activationCodeNotApplicableString = "N/A";
      String myActivationCodeUUIDString = sharedPreferenceGet(getActivity(),
            "SHARED_PREFERENCE_OBSERVER_ID", activationCodeNotApplicableString);
      TextView myActivationCode = ((TextView) view.findViewById(R.id.myActivationCode));
      myActivationCode.setText(" My Activation Code: " + myActivationCodeUUIDString);

      Button mbuttonBackToMain = (Button) view.findViewById(R.id.buttonBackToMain);
      mbuttonBackToMain.setOnClickListener(v ->{
         Fragment fragment = new FragmentMain();

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
      });
      return view;


   }

}