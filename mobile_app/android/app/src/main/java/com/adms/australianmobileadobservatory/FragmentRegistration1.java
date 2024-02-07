package com.adms.australianmobileadobservatory;

import android.os.Bundle;
import android.text.method.LinkMovementMethod;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentTransaction;

public class FragmentRegistration1 extends Fragment {
   private Button mbuttonIAgree;
   private Button mbuttonBackFromRegistration1;

   @Override
   public View onCreateView(LayoutInflater inflater, ViewGroup container,
                            Bundle savedInstanceState) {

         View view = inflater.inflate(R.layout.fragment_registration_1, container, false);


      mbuttonIAgree = (Button) view.findViewById(R.id.buttonIAgree);


      mbuttonBackFromRegistration1 = (Button) view.findViewById(R.id.buttonBackFromRegistration1);

      // TODO - comment
      mbuttonIAgree.setOnClickListener(v ->{
         Fragment fragment = new FragmentRegistration2();

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
      mbuttonBackFromRegistration1.setOnClickListener(v ->{
         Fragment fragment = new FragmentMain();

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
      });


      ((TextView)view.findViewById(R.id.fragment_main_privacy_policy_unregistered)).setMovementMethod(LinkMovementMethod.getInstance());

      return view;


   }

}