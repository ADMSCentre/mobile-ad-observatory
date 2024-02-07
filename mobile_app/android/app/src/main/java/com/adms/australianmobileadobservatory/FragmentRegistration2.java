package com.adms.australianmobileadobservatory;

import static com.adms.australianmobileadobservatory.Settings.sharedPreferenceGet;
import static com.adms.australianmobileadobservatory.Settings.sharedPreferencePut;

import android.app.Activity;
import android.content.DialogInterface;
import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.CompoundButton;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.RadioButton;
import android.widget.RadioGroup;
import android.widget.Switch;
import android.widget.TextView;
import android.widget.Toast;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentTransaction;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;

public class FragmentRegistration2 extends Fragment implements AsyncResponse  {

   Registration asyncTask = new Registration();
   JSONObject registrationJSONObject;
   private DialogLoading loadRegistration;
   private DialogFailedRegistration loadFailedRegistration;
   private DialogSuccessfulRegistration loadSuccessfulRegistration;

   @Override
   public void onCreate(Bundle savedInstanceState) {

      //this to set delegate/listener back to this class
      super.onCreate(savedInstanceState);
      asyncTask.delegate = this;
   }

   @Override
   public void processFinish(Boolean successfulRegistration) {
      if (successfulRegistration) {
         loadRegistration.dismiss();
         sharedPreferencePut(getContext(), "SHARED_PREFERENCE_REGISTERED", "true");
         loadSuccessfulRegistration = new DialogSuccessfulRegistration(requireContext());
         loadSuccessfulRegistration.show();
         loadSuccessfulRegistration.setOnDismissListener(new DialogInterface.OnDismissListener() {
            @Override
            public void onDismiss(DialogInterface dialog) {
               goBack("FragmentMain");
            }
         });
      } else {
         failedRegistration(loadRegistration);
      }
   }

   private static final String TAG = "FragmentRegistration2";
   private Button mbuttonBackFromRegistration2;
   private Button mbuttonCompleteRegistration;

   private Switch mswitchResidingInAustralia;

   public boolean adjustDemographicInputWarnings(View view) {
      boolean inputGenderIncorrect = (
            (((RadioGroup) view.findViewById(R.id.radioGroupGender)).getCheckedRadioButtonId() == -1)
                  || ((((EditText) view.findViewById(R.id.textInputGender)).getText().toString().matches(""))
                  && ((RadioButton) view.findViewById(R.id.radioButtonGenderSpecify)).isChecked()));

      boolean inputAgeIncorrect = (((RadioGroup) view.findViewById(R.id.radioGroupAge)).getCheckedRadioButtonId() == -1);

      boolean inputPostcodeIncorrect = (((EditText) view.findViewById(R.id.textInputPostcode)).getText().toString().matches(""));

      boolean inputEducationIncorrect = (((RadioGroup) view.findViewById(R.id.radioGroupEducation)).getCheckedRadioButtonId() == -1);

      boolean inputIncomeIncorrect = (((RadioGroup) view.findViewById(R.id.radioGroupIncome)).getCheckedRadioButtonId() == -1);

      boolean inputEmploymentIncorrect = (((RadioGroup) view.findViewById(R.id.radioGroupEmployment)).getCheckedRadioButtonId() == -1);

      boolean inputPoliticalPartyPreferenceIncorrect = (
            (((RadioGroup) view.findViewById(R.id.radioGroupPartyPreference)).getCheckedRadioButtonId() == -1)
                  || ((((EditText) view.findViewById(R.id.textInputPoliticalPartyPreference)).getText().toString().matches(""))
                  && ((RadioButton) view.findViewById(R.id.radioButtonPartyPreferenceSpecify)).isChecked()));

      boolean fieldsEmpty = (inputGenderIncorrect ||
            inputAgeIncorrect ||
            inputPostcodeIncorrect ||
            inputEducationIncorrect ||
            inputIncomeIncorrect ||
            inputEmploymentIncorrect ||
            inputPoliticalPartyPreferenceIncorrect);

      // If the gender input is incorrect, display the warning text to rectify it
      if (inputGenderIncorrect) {
         ((TextView) view.findViewById(R.id.warningInputGender)).setVisibility(View.VISIBLE);
      } else {
         ((TextView) view.findViewById(R.id.warningInputGender)).setVisibility(View.GONE);
      }

      // If the age input is incorrect, display the warning text to rectify it
      if (inputAgeIncorrect) {
         ((TextView) view.findViewById(R.id.warningInputAge)).setVisibility(View.VISIBLE);
      } else {
         ((TextView) view.findViewById(R.id.warningInputAge)).setVisibility(View.GONE);
      }

      // If the postcode input is incorrect, display the warning text to rectify it
      if (inputPostcodeIncorrect) {
         ((TextView) view.findViewById(R.id.warningInputPostcode)).setVisibility(View.VISIBLE);
      } else {
         ((TextView) view.findViewById(R.id.warningInputPostcode)).setVisibility(View.GONE);
      }

      // If the education input is incorrect, display the warning text to rectify it
      if (inputEducationIncorrect) {
         ((TextView) view.findViewById(R.id.warningInputEducation)).setVisibility(View.VISIBLE);
      } else {
         ((TextView) view.findViewById(R.id.warningInputEducation)).setVisibility(View.GONE);
      }

      // If the income input is incorrect, display the warning text to rectify it
      if (inputIncomeIncorrect) {
         ((TextView) view.findViewById(R.id.warningInputIncome)).setVisibility(View.VISIBLE);
      } else {
         ((TextView) view.findViewById(R.id.warningInputIncome)).setVisibility(View.GONE);
      }

      // If the employment input is incorrect, display the warning text to rectify it
      if (inputEmploymentIncorrect) {
         ((TextView) view.findViewById(R.id.warningInputEmployment)).setVisibility(View.VISIBLE);
      } else {
         ((TextView) view.findViewById(R.id.warningInputEmployment)).setVisibility(View.GONE);
      }

      // If the political party preference input is incorrect, display the warning text to rectify it
      if (inputPoliticalPartyPreferenceIncorrect) {
         ((TextView) view.findViewById(R.id.warningInputPoliticalPartyPreference)).setVisibility(View.VISIBLE);
      } else {
         ((TextView) view.findViewById(R.id.warningInputPoliticalPartyPreference)).setVisibility(View.GONE);
      }

      return fieldsEmpty;
   }

   private void goBack(String instance) {
      Fragment fragment = null;
      if (instance.equals("FragmentRegistration1")) {
         fragment = new FragmentRegistration1();
      } else
      if (instance.equals("FragmentMain")) {
         fragment = new FragmentMain();
      }

      FragmentTransaction transaction = getParentFragmentManager().beginTransaction();
      transaction.setCustomAnimations(
            R.anim.enter_from_left,  // enter
            R.anim.exit_to_right,  // exit
            R.anim.enter_from_right,   // popEnter
            R.anim.exit_to_left  // popExit
      );
      assert fragment != null;
      transaction.replace(R.id.fragmentContainerView, fragment);
      transaction.addToBackStack(null);
      transaction.commit();
   }

   private void failedRegistration(DialogLoading loadRegistration) {
      Log.i(TAG, "got here");
      loadRegistration.dismiss();
      loadFailedRegistration = new DialogFailedRegistration(requireContext());
      loadFailedRegistration.show();
      loadFailedRegistration.setOnDismissListener(new DialogInterface.OnDismissListener() {
         @Override
         public void onDismiss(DialogInterface dialog) {
            goBack("FragmentMain");
         }
      });
   }

   private String tagOfRadioButton(View view, int thisID) {
      return ((RadioButton) view.findViewById(((RadioGroup) view.findViewById(thisID)).getCheckedRadioButtonId())).getTag().toString();
   }

   @Override
   public View onCreateView(LayoutInflater inflater, ViewGroup container,
                            Bundle savedInstanceState) {


         View view = inflater.inflate(R.layout.fragment_registration_2, container, false);


      mbuttonCompleteRegistration = (Button) view.findViewById(R.id.buttonCompleteRegistration);
      mbuttonCompleteRegistration.setOnClickListener(v ->{

         boolean fieldsEmpty = adjustDemographicInputWarnings(view);

         if (fieldsEmpty) {
            Toast.makeText(getContext(), "One or more fields need to be entered before you can continue", Toast.LENGTH_SHORT).show();
         } else {
            loadRegistration = new DialogLoading(requireContext());
            loadRegistration.show();
            // If the registration value is not yet set
            if (!sharedPreferenceGet(requireContext(),"SHARED_PREFERENCE_REGISTERED", "false").equals("true")) {
               try {
                  String genderResult = tagOfRadioButton(view, R.id.radioGroupGender);
                  String genderSpecifyResult = (((EditText) view.findViewById(R.id.textInputGender)).getText().toString());
                  String ageResult = tagOfRadioButton(view, R.id.radioGroupAge);
                  String postcodeResult = (((EditText) view.findViewById(R.id.textInputPostcode)).getText().toString());
                  String educationResult = tagOfRadioButton(view, R.id.radioGroupEducation);
                  String incomeResult = tagOfRadioButton(view, R.id.radioGroupIncome);
                  String employmentResult = tagOfRadioButton(view, R.id.radioGroupEmployment);
                  String politicalPartyPreferenceResult = tagOfRadioButton(view, R.id.radioGroupPartyPreference);
                  String politicalPartyPreferenceSpecifyResult = (((EditText) view.findViewById(R.id.textInputPoliticalPartyPreference)).getText().toString());
                  String indigeneityResult = String.valueOf(((Switch) view.findViewById(R.id.switchIndigeneity)).isChecked());
                  registrationJSONObject = new JSONObject();
                  registrationJSONObject.put("gender",genderResult);
                  registrationJSONObject.put("genderSpecify",genderSpecifyResult);
                  registrationJSONObject.put("age",ageResult);
                  registrationJSONObject.put("postcode",postcodeResult);
                  registrationJSONObject.put("indigeneity",indigeneityResult);
                  registrationJSONObject.put("education",educationResult);
                  registrationJSONObject.put("income",incomeResult);
                  registrationJSONObject.put("employment",employmentResult);
                  registrationJSONObject.put("politicalPartyPreference",politicalPartyPreferenceResult);
                  registrationJSONObject.put("politicalPartyPreferenceSpecify",politicalPartyPreferenceSpecifyResult);
                  asyncTask.execute(registrationJSONObject);
               } catch (JSONException e) {
                  failedRegistration(loadRegistration);
               }
            }
         }
      });


      mswitchResidingInAustralia = (Switch) view.findViewById(R.id.switchResidingInAustralia);
      mswitchResidingInAustralia.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
         public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
            if (!isChecked) {
               Toast.makeText(getContext(), "Research participation is not available to users outside Australia at this point.", Toast.LENGTH_SHORT).show();
               ((LinearLayout) view.findViewById(R.id.fragmentMainUnregisteredAustralian)).setVisibility(View.GONE);
               ((Button) view.findViewById(R.id.buttonCompleteRegistration)).setVisibility(View.GONE);
               ((TextView) view.findViewById(R.id.textViewAustralianCondition)).setVisibility(View.VISIBLE);
            } else {
               ((LinearLayout) view.findViewById(R.id.fragmentMainUnregisteredAustralian)).setVisibility(View.VISIBLE);
               ((Button) view.findViewById(R.id.buttonCompleteRegistration)).setVisibility(View.VISIBLE);
               ((TextView) view.findViewById(R.id.textViewAustralianCondition)).setVisibility(View.GONE);
            }
         }
      });

      mbuttonBackFromRegistration2 = (Button) view.findViewById(R.id.buttonBackFromRegistration2);
      mbuttonBackFromRegistration2.setOnClickListener(v ->{
         goBack("FragmentRegistration1");
      });

      view.findViewById(R.id.radioButtonSpecifyGenderTextInput).setVisibility(View.GONE);
      RadioGroup radioGroup = (RadioGroup) view.findViewById(R.id.radioGroupGender);
      radioGroup.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener()
      {
         @Override
         public void onCheckedChanged(RadioGroup group, int checkedId) {
            adjustDemographicInputWarnings(view);
            if (checkedId == R.id.radioButtonGenderSpecify) {
               view.findViewById(R.id.radioButtonSpecifyGenderTextInput).setVisibility(View.VISIBLE);
            } else {
               view.findViewById(R.id.radioButtonSpecifyGenderTextInput).setVisibility(View.GONE);
            }
         }
      });

      RadioGroup radioGroupAge = (RadioGroup) view.findViewById(R.id.radioGroupAge);
      radioGroupAge.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener()
      {
         @Override
         public void onCheckedChanged(RadioGroup group, int checkedId) {
            adjustDemographicInputWarnings(view);
         }
      });

      RadioGroup radioGroupEducation = (RadioGroup) view.findViewById(R.id.radioGroupEducation);
      radioGroupEducation.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener()
      {
         @Override
         public void onCheckedChanged(RadioGroup group, int checkedId) {
            adjustDemographicInputWarnings(view);
         }
      });

      RadioGroup radioGroupIncome = (RadioGroup) view.findViewById(R.id.radioGroupIncome);
      radioGroupIncome.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener()
      {
         @Override
         public void onCheckedChanged(RadioGroup group, int checkedId) {
            adjustDemographicInputWarnings(view);
         }
      });

      RadioGroup radioGroupEmployment = (RadioGroup) view.findViewById(R.id.radioGroupEmployment);
      radioGroupEmployment.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener()
      {
         @Override
         public void onCheckedChanged(RadioGroup group, int checkedId) {
            adjustDemographicInputWarnings(view);
         }
      });

      EditText textInputPostcode = ((EditText) view.findViewById(R.id.textInputPostcode));
      textInputPostcode.addTextChangedListener(new TextWatcher() {

         @Override
         public void afterTextChanged(Editable s) {}

         @Override
         public void beforeTextChanged(CharSequence s, int start,
                                       int count, int after) {
         }
         @Override
         public void onTextChanged(CharSequence s, int start,
                                   int before, int count) {
            adjustDemographicInputWarnings(view);
         }
      });

      EditText textInputGender = ((EditText) view.findViewById(R.id.textInputGender));
      textInputGender.addTextChangedListener(new TextWatcher() {

         @Override
         public void afterTextChanged(Editable s) {}

         @Override
         public void beforeTextChanged(CharSequence s, int start,
                                       int count, int after) {
         }
         @Override
         public void onTextChanged(CharSequence s, int start,
                                   int before, int count) {
            adjustDemographicInputWarnings(view);
         }
      });

      EditText textInputPoliticalPartyPreference = ((EditText) view.findViewById(R.id.textInputPoliticalPartyPreference));
      textInputPoliticalPartyPreference.addTextChangedListener(new TextWatcher() {

         @Override
         public void afterTextChanged(Editable s) {}

         @Override
         public void beforeTextChanged(CharSequence s, int start,
                                       int count, int after) {
         }
         @Override
         public void onTextChanged(CharSequence s, int start,
                                   int before, int count) {
            adjustDemographicInputWarnings(view);
         }
      });




      view.findViewById(R.id.radioButtonPartyPreferenceSpecifyTextInput).setVisibility(View.GONE);
      RadioGroup radioGroupPartyPreference = (RadioGroup) view.findViewById(R.id.radioGroupPartyPreference);
      radioGroupPartyPreference.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener()
      {
         @Override
         public void onCheckedChanged(RadioGroup group, int checkedId) {
            adjustDemographicInputWarnings(view);
            if (checkedId == R.id.radioButtonPartyPreferenceSpecify) {
               view.findViewById(R.id.radioButtonPartyPreferenceSpecifyTextInput).setVisibility(View.VISIBLE);
            } else {
               view.findViewById(R.id.radioButtonPartyPreferenceSpecifyTextInput).setVisibility(View.GONE);
            }
         }
      });



      return view;


   }

}